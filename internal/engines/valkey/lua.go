package valkey

import "github.com/valkey-io/valkey-go"

// The scripts build key names from ids. That is fine on a single Valkey
// instance (the benchmark setup) but would not work on a cluster.
//
// Key layout:
//
//	w:{id}        hash   event (nazwa, miasto, data, organizator_id, metadane JSON, cennik JSON, status)
//	wb:{event}    hash   ticket id -> ticket JSON (m seat, s sector, r row, n number, c price, st status, rz reservation)
//	wf:{event}    set    free ticket ids of an event
//	r:{id}        string reservation JSON with embedded tickets and payment
//	k:{id}        hash   customer, preferencje as JSON
//	blok:{ticket} string seat hold with TTL (strategy setnx)
//	sprzedaz      zset   event -> sold tickets
//	przychod      hash   event -> revenue
//	ix:*          secondary structures, only while indexes are on
const prelude = `
-- Valkey's cjson turns an empty JSON array into an empty table, which it
-- encodes back as {}. enc() restores [] for keys that were [] in raw.
local function enc(raw, doc)
  local out = cjson.encode(doc)
  for k in string.gmatch(raw, '"([%w_]+)":%[%]') do
    out = string.gsub(out, '"' .. k .. '":{}', '"' .. k .. '":[]')
  end
  return out
end
local function idx() return redis.call('EXISTS', 'ix:on') == 1 end
local function unindex_res(r)
  redis.call('ZREM', 'ix:k:' .. r.klient_id, r.id)
  redis.call('ZREM', 'ix:r:utw', r.id)
  redis.call('ZREM', 'ix:oczek', r.id)
  redis.call('ZREM', 'ix:wygasle', r.id)
end
local function set_ticket(e, t, st, rz)
  local raw = redis.call('HGET', 'wb:' .. e, t)
  if not raw then return nil end
  local tk = cjson.decode(raw)
  tk.st = st
  tk.rz = rz
  redis.call('HSET', 'wb:' .. e, t, enc(raw, tk))
  return tk
end
`

// reserve: ARGV = resID, resJSON, ticketStatus, customerID, createdMs, expiresMs|'' , n, (event, ticket)*n
var reserveScript = valkey.NewLuaScript(prelude + `
local n = tonumber(ARGV[7])
for i = 0, n - 1 do
  if redis.call('SISMEMBER', 'wf:' .. ARGV[8 + 2 * i], ARGV[9 + 2 * i]) == 0 then return 0 end
end
for i = 0, n - 1 do
  local e, t = ARGV[8 + 2 * i], ARGV[9 + 2 * i]
  redis.call('SREM', 'wf:' .. e, t)
  set_ticket(e, t, ARGV[3], tonumber(ARGV[1]))
end
redis.call('SET', 'r:' .. ARGV[1], ARGV[2])
if tonumber(ARGV[1]) > 2000000000000 then redis.call('SADD', 'contend:res', ARGV[1]) end
if idx() then
  redis.call('ZADD', 'ix:k:' .. ARGV[4], ARGV[5], ARGV[1])
  redis.call('ZADD', 'ix:r:utw', ARGV[5], ARGV[1])
  if ARGV[6] ~= '' then redis.call('ZADD', 'ix:oczek', ARGV[6], ARGV[1]) end
end
return n
`)

// hold: ARGV = event, ticket, untilMs, ttlMs
var holdScript = valkey.NewLuaScript(prelude + `
if redis.call('SREM', 'wf:' .. ARGV[1], ARGV[2]) == 0 then return 0 end
local tk = set_ticket(ARGV[1], ARGV[2], 'zablokowany', nil)
tk.zd = tonumber(ARGV[3])
redis.call('HSET', 'wb:' .. ARGV[1], ARGV[2], cjson.encode(tk))
redis.call('SET', 'blok:' .. ARGV[2], '1', 'PX', ARGV[4])
return 1
`)

// setstatus: ARGV = resID, status, paymentJSON|''
var statusScript = valkey.NewLuaScript(prelude + `
local raw = redis.call('GET', 'r:' .. ARGV[1])
if not raw then return 0 end
local r = cjson.decode(raw)
r.status = ARGV[2]
r.wygasa_o = nil
if ARGV[3] ~= '' then r.platnosc = cjson.decode(ARGV[3]) end
redis.call('SET', 'r:' .. ARGV[1], enc(raw, r))
if idx() then redis.call('ZREM', 'ix:oczek', ARGV[1]) end
return 1
`)

// release: ARGV = resID, mode, expiresMs. Modes: 'cancel' frees the tickets
// and deletes the reservation, 'drop' deletes both, 'expire' frees the
// tickets and keeps the reservation with status wygasla.
// Returns the number of tickets touched, or -1 when the reservation is gone.
var releaseScript = valkey.NewLuaScript(prelude + `
local raw = redis.call('GET', 'r:' .. ARGV[1])
if not raw then return -1 end
local r = cjson.decode(raw)
local mode = ARGV[2]
local n = 0
if type(r.bilety) == 'table' then
  for _, b in ipairs(r.bilety) do
    local e, t = b.wydarzenie_id, b.bilet_id
    if mode == 'drop' then
      if redis.call('HDEL', 'wb:' .. e, t) == 1 then n = n + 1 end
      redis.call('SREM', 'wf:' .. e, t)
    else
      if set_ticket(e, t, 'wolny', nil) then n = n + 1 end
      redis.call('SADD', 'wf:' .. e, t)
    end
    if r.status == 'oplacona' then
      redis.call('ZINCRBY', 'sprzedaz', -1, e)
      redis.call('HINCRBYFLOAT', 'przychod', e, -b.cena)
    end
  end
end
if mode == 'expire' then
  r.status = 'wygasla'
  redis.call('SET', 'r:' .. ARGV[1], enc(raw, r))
  if idx() then
    redis.call('ZREM', 'ix:oczek', ARGV[1])
    redis.call('ZADD', 'ix:wygasle', ARGV[3], ARGV[1])
  end
else
  redis.call('DEL', 'r:' .. ARGV[1])
  if idx() then unindex_res(r) end
end
return n
`)

// sectorprice: ARGV = event, sector, factor. Returns tickets changed.
var sectorPriceScript = valkey.NewLuaScript(prelude + `
local all = redis.call('HGETALL', 'wb:' .. ARGV[1])
local sector, f, n = tonumber(ARGV[2]), tonumber(ARGV[3]), 0
for i = 1, #all, 2 do
  local tk = cjson.decode(all[i + 1])
  if tk.s == sector and tk.st == 'wolny' then
    tk.c = math.floor(tk.c * f * 100 + 0.5) / 100
    redis.call('HSET', 'wb:' .. ARGV[1], all[i], enc(all[i + 1], tk))
    n = n + 1
  end
end
local craw = redis.call('HGET', 'w:' .. ARGV[1], 'cennik')
local cennik = cjson.decode(craw)
for _, p in ipairs(cennik) do
  if p.sektor_id == sector then p.cena = math.floor(p.cena * f * 100 + 0.5) / 100 end
end
redis.call('HSET', 'w:' .. ARGV[1], 'cennik', enc(craw, cennik))
return n
`)

// hjson: KEYS[1] hash; ARGV = field, op ('merge' object patch | 'push' path value), arg1, arg2
var hjsonScript = valkey.NewLuaScript(prelude + `
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local doc = cjson.decode(raw)
if ARGV[2] == 'merge' then
  for k, v in pairs(cjson.decode(ARGV[3])) do doc[k] = v end
else
  table.insert(doc[ARGV[3]], ARGV[4])
end
redis.call('HSET', KEYS[1], ARGV[1], enc(raw, doc))
return 1
`)

// dropevent: ARGV = event, requiredStatus|''. Returns tickets deleted or -1.
var dropEventScript = valkey.NewLuaScript(prelude + `
local e = ARGV[1]
local ev = redis.call('HMGET', 'w:' .. e, 'status', 'miasto', 'organizator_id', 'gatunek')
if not ev[1] then return -1 end
if ARGV[2] ~= '' and ev[1] ~= ARGV[2] then return -1 end
local n = redis.call('HLEN', 'wb:' .. e)
redis.call('DEL', 'wb:' .. e, 'wf:' .. e, 'w:' .. e)
redis.call('SREM', 'w:all', e)
redis.call('ZREM', 'sprzedaz', e)
redis.call('HDEL', 'przychod', e)
if idx() then
  redis.call('ZREM', 'ix:miasto:' .. ev[2], e)
  redis.call('SREM', 'ix:org:' .. ev[3], e)
  redis.call('SREM', 'ix:gatunek:' .. ev[4], e)
  redis.call('ZREM', 'ix:w:data', e)
end
return n
`)

// contend:verify: ARGV = event, ticket ids... Returns {reserved, reservations}.
var verifyScript = valkey.NewLuaScript(prelude + `
local reserved = 0
for i = 2, #ARGV do
  local raw = redis.call('HGET', 'wb:' .. ARGV[1], ARGV[i])
  if raw then
    local tk = cjson.decode(raw)
    if tk.st == 'zarezerwowany' and tk.rz and tk.rz > 2000000000000 then reserved = reserved + 1 end
  end
end
local res = 0
for _, id in ipairs(redis.call('SMEMBERS', 'contend:res')) do
  res = res + redis.call('EXISTS', 'r:' .. id)
end
return {reserved, res}
`)
