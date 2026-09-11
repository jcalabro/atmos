// Generates Phase 1 interoperability fixtures. The algorithms and wire layout
// are transcribed from bluesky-social/atproto PR #5187 at commit
// 9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33. Dependencies are exact-pinned by
// package-lock.json. CI consumes the generated artifacts without Node/network.
import { writeFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { fileURLToPath } from 'node:url'
import { p256 } from '@noble/curves/p256'
import { secp256k1 } from '@noble/curves/secp256k1'
import { blake3 } from '@noble/hashes/blake3'
import { expand } from '@noble/hashes/hkdf'
import { hmac } from '@noble/hashes/hmac'
import { sha256 } from '@noble/hashes/sha256'

const sourceCommit = '9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33'
const encoder = new TextEncoder()
const outDir = fileURLToPath(new URL('..', import.meta.url))

const concat = (...parts) => {
  const size = parts.reduce((n, part) => n + part.length, 0)
  const out = new Uint8Array(size)
  let offset = 0
  for (const part of parts) {
    out.set(part, offset)
    offset += part.length
  }
  return out
}
const hex = (bytes) => Buffer.from(bytes).toString('hex')
const b64 = (bytes) => Buffer.from(bytes).toString('base64')
const utf8 = (value) => encoder.encode(value)
const uint = (value) => {
  if (value < 24) return Uint8Array.of(value)
  if (value <= 0xff) return Uint8Array.of(0x18, value)
  if (value <= 0xffff) return Uint8Array.of(0x19, value >>> 8, value & 0xff)
  throw new Error(`integer too large for fixture encoder: ${value}`)
}
const bytes = (value) => concat(majorLength(2, value.length), value)
const text = (value) => {
  const encoded = utf8(value)
  return concat(majorLength(3, encoded.length), encoded)
}
const array = (values) => concat(majorLength(4, values.length), ...values)
const map = (entries) => {
  const sorted = [...entries].sort(([a], [b]) => {
    const aa = utf8(a)
    const bb = utf8(b)
    return aa.length - bb.length || Buffer.compare(aa, bb)
  })
  return concat(majorLength(5, sorted.length), ...sorted.flatMap(([key, value]) => [text(key), value]))
}
function majorLength(major, length) {
  const head = major << 5
  if (length < 24) return Uint8Array.of(head | length)
  if (length <= 0xff) return Uint8Array.of(head | 24, length)
  if (length <= 0xffff) return Uint8Array.of(head | 25, length >>> 8, length & 0xff)
  if (length <= 0xffffffff) {
    return Uint8Array.of(head | 26, length >>> 24, length >>> 16, length >>> 8, length)
  }
  throw new Error(`length too large for fixture encoder: ${length}`)
}
const varint = (input) => {
  let value = input
  const out = []
  while (value >= 0x80) {
    out.push((value & 0x7f) | 0x80)
    value = Math.floor(value / 128)
  }
  out.push(value)
  return Uint8Array.from(out)
}
const digest = (value) => new Uint8Array(createHash('sha256').update(value).digest())
const cid = (value) => concat(Uint8Array.of(0x01, 0x71, 0x12, 0x20), digest(value))
const cidLink = (value) => concat(Uint8Array.of(0xd8, 0x2a), bytes(concat(Uint8Array.of(0), value)))
const carBlock = (blockCid, data) => {
  const body = concat(blockCid, data)
  return concat(varint(body.length), body)
}

class LtHash {
  constructor() {
    this.state = new Uint8Array(2048)
    this.lanes = new Uint16Array(this.state.buffer)
  }
  apply(element, direction) {
    const expanded = blake3(utf8(element), { dkLen: 2048 })
    const copied = new Uint8Array(2048)
    copied.set(expanded)
    const lanes = new Uint16Array(copied.buffer)
    for (let i = 0; i < 1024; i++) this.lanes[i] += direction * lanes[i]
  }
  add(element) { this.apply(element, 1) }
  remove(element) { this.apply(element, -1) }
  digest() { return sha256(this.state) }
}

const encodeContext = ({ space, author, rev }, ikm) => {
  const fields = [utf8(space), utf8(author), utf8(rev), ikm]
  const prefixed = fields.map((field) => concat(Uint8Array.of(field.length >>> 8, field.length), field))
  return concat(utf8('atproto-space-v1'), ...prefixed)
}
const computeMac = (ikm, context, hash) => hmac(sha256, expand(sha256, ikm, context, 32), hash)

const record = map([
  ['text', text('hello from a fixed spaces fixture')],
  ['$type', text('com.example.post')],
])
const recordCid = cid(record)
// Distinct paths deliberately share one CID; the CAR must carry two blocks.
const paths = ['com.example.post/a', 'com.example.longer/b']
const index = map(paths.map((path) => [path, cidLink(recordCid)]))
const indexCid = cid(index)
const context = {
  space: 'at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.forum/3jzfcijpj2z2a',
  author: 'did:plc:bbbbbbbbbbbbbbbbbbbbbbbb',
  rev: '3jzfcijpj2z2a',
}
const ikm = Uint8Array.from({ length: 32 }, (_, i) => i)

// CID strings are lower-case base32 without padding, not base64url.
const alphabet = 'abcdefghijklmnopqrstuvwxyz234567'
const base32 = (data) => {
  let bits = 0
  let value = 0
  let out = ''
  for (const byte of data) {
    value = (value << 8) | byte
    bits += 8
    while (bits >= 5) {
      out += alphabet[(value >>> (bits - 5)) & 31]
      bits -= 5
    }
  }
  if (bits) out += alphabet[(value << (5 - bits)) & 31]
  return out
}
const recordCidString = `b${base32(recordCid)}`
const repoHash = new LtHash()
for (const path of paths) repoHash.add(`${path}/${recordCidString}`)
const ctxBytes = encodeContext(context, ikm)
const mac = computeMac(ikm, ctxBytes, repoHash.digest())

const privateKey = Uint8Array.from({ length: 32 }, (_, i) => (i === 31 ? 1 : 0))
const keyFamilies = [
  ['p256', p256],
  ['k256', secp256k1],
]
const commits = []
for (const [name, curve] of keyFamilies) {
  const signature = curve.sign(sha256(ctxBytes), privateKey, { lowS: true }).toCompactRawBytes()
  const publicKey = curve.getPublicKey(privateKey, true)
  const commit = map([
    ['ver', uint(1)],
    ['hash', bytes(repoHash.digest())],
    ['ikm', bytes(ikm)],
    ['mac', bytes(mac)],
    ['sig', bytes(signature)],
    ['rev', text(context.rev)],
  ])
  const commitCid = cid(commit)
  const header = map([
    ['roots', array([cidLink(commitCid), cidLink(indexCid)])],
    ['version', uint(1)],
  ])
  const car = concat(
    varint(header.length), header,
    carBlock(commitCid, commit),
    carBlock(indexCid, index),
    carBlock(recordCid, record),
    carBlock(recordCid, record),
  )
  const filename = `repo-${name}.car`
  await writeFile(new URL(`../${filename}`, import.meta.url), car)
  commits.push({
    keyFamily: name,
    publicKeyHex: hex(publicKey),
    signatureHex: hex(signature),
    commitCborBase64: b64(commit),
    commitCid: `b${base32(commitCid)}`,
    carFile: filename,
    carSha256: hex(digest(car)),
  })
}

const lt = new LtHash()
const ltVectors = [{ operation: 'empty', digestHex: hex(lt.digest()), stateBase64: b64(lt.state) }]
lt.add('alpha')
ltVectors.push({ operation: 'add alpha', digestHex: hex(lt.digest()), stateBase64: b64(lt.state) })
lt.add('snowman-☃')
ltVectors.push({ operation: 'add snowman', digestHex: hex(lt.digest()), stateBase64: b64(lt.state) })
lt.add('alpha')
ltVectors.push({ operation: 'add duplicate alpha', digestHex: hex(lt.digest()), stateBase64: b64(lt.state) })
lt.remove('alpha')
ltVectors.push({ operation: 'remove alpha', digestHex: hex(lt.digest()), stateBase64: b64(lt.state) })

const manifest = {
  provenance: {
    upstreamRepo: 'https://github.com/bluesky-social/atproto',
    upstreamPullRequest: 5187,
    upstreamCommit: sourceCommit,
    nobleHashes: '1.7.0',
    nobleCurves: '1.7.0',
    generator: 'space/testdata/generate/generate.mjs',
  },
  ltHash: ltVectors,
  context: {
    ...context,
    ikmHex: hex(ikm),
    contextHex: hex(ctxBytes),
    hashHex: hex(repoHash.digest()),
    macHex: hex(mac),
  },
  record: {
    paths,
    cid: recordCidString,
    cborBase64: b64(record),
  },
  commits,
}
await writeFile(new URL('../space-vectors.json', import.meta.url), `${JSON.stringify(manifest, null, 2)}\n`)
