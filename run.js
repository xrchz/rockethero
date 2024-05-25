import 'dotenv/config'
import { ethers } from 'ethers'
import { open } from 'lmdb'
import http from 'node:http'
import express from 'express'

// environment variables:
// RPC: URL endpoint for archive node JSON-RPC (default: http://localhost:8545)
// DB_DIR: custom path to rockethero database (default: db)
// MAX_QUERY_RANGE: maximum range for querying logs (default: 1000)
// PORT: port to listen for http requests on (default: 8888)

const timestamp = () => Intl.DateTimeFormat(
    'en-GB', {dateStyle: 'medium', timeStyle: 'medium'}
  ).format(new Date())

const log = s => console.log(`${timestamp()} ${s}`)

const MAX_QUERY_RANGE = process.env.MAX_QUERY_RANGE | 1000

const provider = new ethers.JsonRpcProvider(process.env.RPC || 'http://localhost:8545')
const db = open({path: process.env.DB_DIR || 'db'})

const rocketStorageGenesisBlock = 13325233

const rocketStorage = new ethers.Contract(
  await provider.resolveName('rocketstorage.eth'),
  ['function getAddress(bytes32 _key) view returns (address)',
   'event NodeWithdrawalAddressSet (address indexed node, address indexed withdrawalAddress, uint256 time)'],
  provider
)
log(`Rocket Storage: ${await rocketStorage.getAddress()}`)

const rocketMinipoolManager = new ethers.Contract(
  await rocketStorage['getAddress(bytes32)'](ethers.id('contract.addressrocketMinipoolManager')),
  ['function getMinipoolExists(address) view returns (bool)'],
  provider
)
log(`Rocket Minipool Manager: ${await rocketMinipoolManager.getAddress()}`)

let withdrawalAddressBlock = db.get(['withdrawalAddressBlock'])
if (!withdrawalAddressBlock) withdrawalAddressBlock = rocketStorageGenesisBlock

async function updateWithdrawalAddresses(finalizedBlockNumber) {
  while (withdrawalAddressBlock < finalizedBlockNumber) {
    const min = withdrawalAddressBlock
    const max = Math.min(withdrawalAddressBlock + MAX_QUERY_RANGE, finalizedBlockNumber)
    log(`Processing withdrawal addresses ${min}...${max}`)
    const logs = await rocketStorage.queryFilter('NodeWithdrawalAddressSet', min, max)
    for (const entry of logs) {
      const nodeAddress = entry.args[0].toLowerCase()
      const withdrawalAddress = entry.args[1]
      const key = ['withdrawalAddressFor',nodeAddress]
      log(`Updating withdrawalAddress ${withdrawalAddress} for nodeAddress ${nodeAddress}`)
      await db.put(key, withdrawalAddress)
    }
    withdrawalAddressBlock = max
    await db.put(['withdrawalAddressBlock'], withdrawalAddressBlock)
  }
}

let blockLock
provider.addListener('block', async () => {
  if (!blockLock) {
    const finalizedBlockNumber = await provider.getBlock('finalized').then(b => b.number)
    blockLock = updateWithdrawalAddresses(finalizedBlockNumber)
    await blockLock
    blockLock = false
  }
})

const app = express()
app.use(express.json())

const nullAddress = '0x'.padEnd(42, '0')
const addressRE = new RegExp('0x[0-9a-fA-F]{40}')

app.post('/', async (req, res, next) => {
  try {
    if (!(req.body instanceof Array && req.body.every(x => typeof x == 'string' && addressRE.test(x))))
      return res.status(400).send('expected array of addresses')
    const output = await Promise.all(
      req.body.map(async address => {
        const result = {withdrawal_credential: address}
        if (await rocketMinipoolManager.getMinipoolExists(address)) {
          const minipool = new ethers.Contract(address, ['function getNodeAddress() view returns (address)'], provider)
          const nodeAddress = await minipool.getNodeAddress() // TODO: could also cache this
          const withdrawalAddress = db.get(['withdrawalAddressFor', nodeAddress.toLowerCase()])
          result.rp_withdrawal_address = withdrawalAddress || nodeAddress
        }
        else result.rp_withdrawal_address = nullAddress
        return result
      })
    )
    return res.json(output)
  }
  catch (e) { next(e) }
})

const server = http.createServer(app)
server.listen(process.env.PORT || 8888)
