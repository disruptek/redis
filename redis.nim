import std/math
import std/net
import std/options
import std/parseutils
import std/sequtils
import std/strutils
import std/tables
import std/times

const
  redisNil* = "\0\0"
  redisCl* = "\r\n"
  redisDol* = "$"

type
  Pipeline = object
    enabled: bool
    buffer: string
    expected: int ## number of replies expected if pipelined

  Redis* = object
    socket: Socket
    pipeline: Pipeline

  RedisStatus* = distinct string
  RedisError* = object of IOError    ## Error in redis
  ReplyError* = object of RedisError ## Invalid reply from redis
  WatchError* = object of RedisError ## Watch error in redis

  RedisCursor* = distinct BiggestInt

proc isConnected*(r: Redis): bool =
  ## true if the client is connected to a redis server
  not r.socket.isNil

proc close*(r: var Redis) =
  ## disconnect from redis
  if r.isConnected:
    close r.socket
    r.socket = nil

proc `=destroy`*(r: var Redis) = close r

proc `$`*(cursor: RedisCursor): string {.borrow.}
proc `==`*(a, b: RedisCursor): bool {.borrow.}

proc `$`*(status: RedisStatus): string {.borrow.}
proc `==`*(a, b: RedisStatus): bool {.borrow.}

proc openRedis*(host = "localhost", port = 6379.Port): Redis =
  ## Open a synchronous connection to a redis server.
  result.socket = newSocket(buffered = true)
  try:
    result.socket.connect(host, port)
  except OSError as e:
    result.socket = nil       # we are disconnected; tear down socket
    raise RedisError.newException:
      "unable to connect to redis at $#:$#: $#" % [ host, $port, e.msg ]

template safeSocket(r: Redis; body: untyped): untyped =
  if r.isConnected:
    body
  else:
    raise RedisError.newException "not connected to redis"

proc managedSend(r: Redis, data: string) =
  safeSocket r:
    r.socket.send(data)

proc managedRecv(r: Redis, size: int): string =
  safeSocket r:
    result = newString(size)
    if r.socket.recv(result, size) != size:
      raise ReplyError.newException "short recv"

template managedRecvLine(r: Redis): string =
  safeSocket r:
    if r.pipeline.enabled:
      ""
    else:
      recvLine r.socket

template raiseInvalidReply(r: Redis, expected, got: char) =
  raise ReplyError.newException:
    "Expected '$1' at the beginning of a status reply got '$2'" %
      [$expected, $got]

proc parseStatus(r: Redis, line = ""): RedisStatus =
  if r.pipeline.enabled:
    result = RedisStatus"PIPELINED"
  else:
    if line == "":
      raise RedisError.newException:
        "Server closed connection prematurely"
    else:
      case line[0]
      of '-':
        raise ReplyError.newException line[1..^1]
      of '+':
        result = RedisStatus line[1..^1] # Strip '+'
      else:
        raise ReplyError.newException line

proc readStatus(r: Redis): RedisStatus =
  r.parseStatus r.managedRecvLine()

proc raiseUnlessOkay(r: Redis) =
  template pipelined: bool = r.pipeline.enabled
  let status = readStatus r
  if pipelined:
    if $status notin ["QUEUED", "PIPELINED"]:
      raise ReplyError.newException:
        "Expected `QUEUED` or `PIPELINED` got `$1`" % [$status]
  elif $status != "OK":
    raise ReplyError.newException "Expected `OK` got `$1`" % [$status]

proc parseInteger(r: Redis, line = ""): BiggestInt =
  if r.pipeline.enabled:
    return -1

  #if line == "+QUEUED":  # inside of multi
  #  return -1

  if line == "":
    raise RedisError.newException:
      "Server closed connection prematurely"

  if line[0] == '-':
    raise RedisError.newException line
  if line[0] != ':':
    raiseInvalidReply(r, ':', line[0])

  # Strip ':'
  if parseBiggestInt(line, result, 1) == 0:
    raise ReplyError.newException "Unable to parse integer."

proc readInteger(r: Redis): BiggestInt =
  let line = r.managedRecvLine()
  if line.len == 0:
    return -1

  result = r.parseInteger(line)

proc readSingleString(r: Redis, line: string, allowMBNil: bool): Option[string] =
  if r.pipeline.enabled:
    return

  # Error.
  if line[0] == '-':
    raise RedisError.newException line

  # Some commands return a /bulk/ value or a /multi-bulk/ nil. Odd.
  if allowMBNil:
    if line == "*-1":
       return

  if line[0] != '$':
    raiseInvalidReply(r, '$', line[0])

  var numBytes = parseInt(line.substr(1))
  if numBytes == -1:
    return

  var s = r.managedRecv(numBytes + 2)
  s.setLen max(0, s.len - 2)
  result = some(s)

when false:
  proc readSingleString(r: Redis): string =
    # TODO: Rename these style of procedures to `processSingleString`?
    let line = r.managedRecvLine()
    if line.len == 0:
      return ""

    let res = r.readSingleString(line, allowMBNil = false)
    result = res.get(redisNil)

proc readNext(r: var Redis): seq[string]
proc readArrayLines(r: var Redis, countLine: string): seq[string] =
  if countLine[0] != '*':
    raiseInvalidReply(r, '*', countLine[0])

  var numElems = parseInt(countLine.substr(1))
  result = @[]

  if numElems == -1:
    return result

  for i in 1..numElems:
    var parsed = r.readNext()

    if parsed.len > 0:
      for item in parsed:
        result.add(item)

when false:
  proc readArrayLines(r: Redis): seq[string] =
    let line = managedRecvLine r
    if line.len == 0:
      return @[]

    result = r.readArrayLines(line)

proc readBulkString(r: Redis, allowMBNil = false): string =
  result = managedRecvLine r
  if result.len == 0: return ""

  let o = r.readSingleString(result, allowMBNil)
  result = o.get redisNil

proc readArray(r: var Redis): seq[string] =
  let line = managedRecvLine r
  if line.len == 0 or line == "+QUEUED":
    return @[]

  result = r.readArrayLines(line)

proc readArrayToTable(r: var Redis): Table[string, string] =
  let arr = readArray r
  for i in countup(0, arr.high, 2):
    result[arr[i]] = arr[i + 1]

proc readNext(r: var Redis): seq[string] =
  let tipe = r.managedRecv(1)
  if tipe.len == 0: return @[]

  let line = managedRecvLine r

  try:
    case tipe[0]
    of '+':
      result = @[line]
    of '-':
      raise ReplyError.newException line
    of ':':
      result = @[$(r.parseInteger(tipe & line))]
    of '$':
      let x = r.readSingleString(tipe & line, true)
      result = @[x.get(redisNil)]
    of '*':
      result = r.readArrayLines(tipe & line)
    else:
      raise ReplyError.newException "readNext failed on line: " & line
  finally:
    r.pipeline.expected -= 1

proc flushPipeline*(r: var Redis, wasMulti = false): seq[string] =
  ## Send buffered commands, clear buffer, return results
  safeSocket r:
    if r.pipeline.buffer.len > 0:
      r.socket.send(r.pipeline.buffer)
  r.pipeline.buffer.setLen(0)
  r.pipeline.enabled = false

  let expected = r.pipeline.expected

  if not wasMulti:
    for i in 0 ..< expected:
      for item in r.readNext():
        if "OK" notin item and "QUEUED" notin item:
          result.add(item)
    r.pipeline.expected = 0
    return

  for i in 0 ..< expected - 1:
    discard r.readNext()

  let ret = r.readNext()
  result = ret.filterIt("OK" notin it)

  r.pipeline.expected = 0
  if ret.len < expected - 2:
    raise WatchError.newException "Watched keys changed during transaction"

proc startPipelining*(r: var Redis) =
  ## Enable command pipelining (reduces network roundtrips).
  ## Note that when enabled, you must call flushPipeline to actually send commands, except
  ## for multi/exec() which enable and flush the pipeline automatically.
  ## Commands return immediately with dummy values; actual results returned from
  ## flushPipeline() or exec()
  if not r.pipeline.enabled:
    r.pipeline.expected = 0
    r.pipeline.enabled = true

proc queuePipeline*(r: var Redis, data: string) =
  r.pipeline.buffer.add(data)
  r.pipeline.expected += 1
  safeSocket r:
    if r.pipeline.buffer.len >= 8192:
      r.socket.send(r.pipeline.buffer)
      r.pipeline.buffer.setLen(0)

template fmtArg(str: string): untyped {.dirty.} =
  $str.len & "\r\n" & str & "\r\n"

proc sendCommand(r: var Redis, cmd: string) =
  var request = "*1\r\n"
  request.add("$" & $cmd.len() & "\r\n")
  request.add(cmd & "\r\n")

  if r.pipeline.enabled:
    r.queuePipeline(request)
  else:
    r.managedSend(request)

proc sendCommand(r: var Redis, cmd: string, args: openArray[string]) =
  var request = "*" & $(1 + args.len()) & "\r\n"
  request.add("$" & $cmd.len() & "\r\n")
  request.add(cmd & "\r\n")
  for i in args:
    request.add("$" & $i.len() & "\r\n")
    request.add(i & "\r\n")

  if r.pipeline.enabled:
    r.queuePipeline(request)
  else:
    r.managedSend(request)

proc sendCommand(r: var Redis, cmd: string, arg1: string) =
  var request = "*2\r\n"
  request.add("$" & $cmd.len() & "\r\n")
  request.add(cmd & "\r\n")
  request.add("$" & $arg1.len() & "\r\n")
  request.add(arg1 & "\r\n")

  if r.pipeline.enabled:
    r.queuePipeline(request)
  else:
    r.managedSend(request)

proc sendCommand(r: var Redis, cmd, key, val: string) =
  let request = "*3" & "\r\n" & "$" &
    fmtArg(cmd) & "$" & fmtArg(key) & "$" & fmtArg(val)

  if r.pipeline.enabled:
    r.queuePipeline(request)
  else:
    r.managedSend(request)

proc sendCommand(r: var Redis, cmd: string, arg1: string, args: openArray[string]) =
  var request = "*" & $(2 + args.len()) & redisCl & redisDol &
    fmtArg(cmd) & redisDol & fmtArg(arg1)

  for i in args:
    request.add(redisDol & fmtArg(i))

  if r.pipeline.enabled:
    r.queuePipeline(request)
  else:
    r.managedSend(request)

# Keys

proc del*(r: var Redis, key: string): BiggestInt =
  ## Delete a key
  r.sendCommand("DEL", [key])
  result = r.readInteger()

proc del*(r: var Redis, keys: seq[string]): BiggestInt =
  ## Delete keys
  r.sendCommand("DEL", keys)
  result = r.readInteger()

proc exists*(r: var Redis, key: string): bool =
  ## Determine if a key exists
  r.sendCommand("EXISTS", [key])
  result = (r.readInteger()) == 1

proc expire*(r: var Redis, key: string, seconds: int): bool =
  ## Set a key's time to live in seconds. Returns `false` if the key could
  ## not be found or the timeout could not be set.
  r.sendCommand("EXPIRE", key, [$seconds])
  result = (r.readInteger()) == 1

proc expireAt*(r: var Redis, key: string, timestamp: int): bool =
  ## Set the expiration for a key as a UNIX timestamp. Returns `false`
  ## if the key could not be found or the timeout could not be set.
  r.sendCommand("EXPIREAT", key, [$timestamp])
  result = (r.readInteger()) == 1

proc keys*(r: var Redis, pattern: string): seq[string] =
  ## Find all keys matching the given pattern
  r.sendCommand("KEYS", pattern)
  result = r.readArray()

proc scan*(r: var Redis, cursor: var RedisCursor): seq[string] =
  ## Find all keys matching the given pattern and yield it to client in portions
  ## using default Redis values for MATCH and COUNT parameters
  r.sendCommand("SCAN", $cursor)
  let reply = r.readArray()
  cursor = RedisCursor parseBiggestInt(reply[0])
  result = reply[1..high(reply)]

proc scan*(r: var Redis, cursor: var RedisCursor, pattern: string): seq[string] =
  ## Find all keys matching the given pattern and yield it to client in portions
  ## using cursor as a client query identifier. Using default Redis value for COUNT argument
  r.sendCommand("SCAN", $cursor, ["MATCH", pattern])
  let reply = r.readArray()
  cursor = RedisCursor parseBiggestInt(reply[0])
  result = reply[1..high(reply)]

proc scan*(r: var Redis, cursor: var RedisCursor, pattern: string, count: int): seq[string] =
  ## Find all keys matching the given pattern and yield it to client in portions
  ## using cursor as a client query identifier.
  r.sendCommand("SCAN", $cursor, ["MATCH", pattern, "COUNT", $count])
  let reply = r.readArray()
  cursor = RedisCursor parseBiggestInt(reply[0])
  result = reply[1..high(reply)]

proc move*(r: var Redis, key: string, db: int): bool =
  ## Move a key to another database. Returns `true` on a successful move.
  r.sendCommand("MOVE", key, [$db])
  result = (r.readInteger()) == 1

proc persist*(r: var Redis, key: string): bool =
  ## Remove the expiration from a key.
  ## Returns `true` when the timeout was removed.
  r.sendCommand("PERSIST", key)
  return (r.readInteger()) == 1

proc randomKey*(r: var Redis): string =
  ## Return a random key from the keyspace
  r.sendCommand("RANDOMKEY")
  result = r.readBulkString()

proc rename*(r: var Redis, key, newkey: string): RedisStatus =
  ## Rename a key.
  ##
  ## **WARNING:** Overwrites `newkey` if it exists!
  r.sendCommand("RENAME", key, [newkey])
  raiseUnlessOkay r

proc renameNX*(r: var Redis, key, newkey: string): bool =
  ## Same as ``rename`` but doesn't continue if `newkey` exists.
  ## Returns `true` if key was renamed.
  r.sendCommand("RENAMENX", key, [newkey])
  result = (r.readInteger()) == 1

proc ttl*(r: var Redis, key: string): BiggestInt =
  ## Get the time to live for a key
  r.sendCommand("TTL", key)
  return r.readInteger()

proc keyType*(r: var Redis, key: string): RedisStatus =
  ## Determine the type stored at key
  r.sendCommand("TYPE", key)
  result = r.readStatus()


# Strings

proc append*(r: var Redis, key, value: string): BiggestInt =
  ## Append a value to a key
  r.sendCommand("APPEND", key, [value])
  result = r.readInteger()

proc decr*(r: var Redis, key: string): BiggestInt =
  ## Decrement the integer value of a key by one
  r.sendCommand("DECR", key)
  result = r.readInteger()

proc decrBy*(r: var Redis, key: string, decrement: int): BiggestInt =
  ## Decrement the integer value of a key by the given number
  r.sendCommand("DECRBY", key, [$decrement])
  result = r.readInteger()

proc mget*(r: var Redis, keys: seq[string]): seq[string] =
  ## Get the values of all given keys
  r.sendCommand("MGET", keys)
  result = r.readArray()

proc get*(r: var Redis, key: string): string =
  ## Get the value of a key. Returns `redisNil` when `key` doesn't exist.
  r.sendCommand("GET", key)
  result = r.readBulkString()

#TODO: BITOP
proc getBit*(r: var Redis, key: string, offset: int): BiggestInt =
  ## Returns the bit value at offset in the string value stored at key
  r.sendCommand("GETBIT", key, [$offset])
  result = r.readInteger()

proc bitCount*(r: var Redis, key: string, limits: seq[string]): BiggestInt =
  ## Returns the number of set bits, optionally within limits
  r.sendCommand("BITCOUNT", key, limits)
  result = r.readInteger()

proc bitPos*(r: var Redis, key: string, bit: int, limits: seq[string]): BiggestInt =
  ## Returns position of the first occurence of bit within limits
  var parameters: seq[string]
  newSeq(parameters, len(limits) + 1)
  parameters.add($bit)
  parameters.add(limits)

  r.sendCommand("BITPOS", key, parameters)
  result = r.readInteger()

proc getRange*(r: var Redis, key: string, start, stop: int): string =
  ## Get a substring of the string stored at a key
  r.sendCommand("GETRANGE", key, [$start, $stop])
  result = r.readBulkString()

proc getSet*(r: var Redis, key: string, value: string): string =
  ## Set the string value of a key and return its old value. Returns `redisNil`
  ## when key doesn't exist.
  r.sendCommand("GETSET", key, [value])
  result = r.readBulkString()

proc incr*(r: var Redis, key: string): BiggestInt =
  ## Increment the integer value of a key by one.
  r.sendCommand("INCR", key)
  result = r.readInteger()

proc incrBy*(r: var Redis, key: string, increment: int): BiggestInt =
  ## Increment the integer value of a key by the given number
  r.sendCommand("INCRBY", key, [$increment])
  result = r.readInteger()

#TODO incrbyfloat

proc msetk*(r: var Redis, keyValues: seq[(string, string)]) =
  ## Set mupltiple keys to multiple values
  var args: seq[string] = @[]
  for key, value in keyValues.items:
    args.add(key)
    args.add(value)
  r.sendCommand("MSET", args)
  raiseUnlessOkay r

proc setk*(r: var Redis, key, value: string) =
  ## Set the string value of a key.
  ##
  ## NOTE: This function had to be renamed due to a clash with the `set` type.
  r.sendCommand("SET", key, value)
  raiseUnlessOkay r

proc setKeyWithExpire*(r: var Redis, key: string; seconds: Positive;
                       value: string) =
  ## Set the string value of a key with expiration in the same round-trip.
  r.sendCommand("SET", key, [value, "EX", $seconds])
  raiseUnlessOkay r

proc setNX*(r: var Redis, key, value: string): bool =
  ## Set the value of a key, only if the key does not exist. Returns `true`
  ## if the key was set.
  r.sendCommand("SETNX", key, [value])
  result = (r.readInteger()) == 1

proc setBit*(r: var Redis, key: string, offset: int,
             value: string): BiggestInt =
  ## Sets or clears the bit at offset in the string value stored at key
  r.sendCommand("SETBIT", key, [$offset, value])
  result = r.readInteger()

proc setEx*(r: var Redis, key: string, seconds: int, value: string): RedisStatus =
  ## Set the value and expiration of a key
  r.sendCommand("SETEX", key, [$seconds, value])
  raiseUnlessOkay r

proc setRange*(r: var Redis, key: string, offset: int,
               value: string): BiggestInt =
  ## Overwrite part of a string at key starting at the specified offset
  r.sendCommand("SETRANGE", key, [$offset, value])
  result = r.readInteger()

proc strlen*(r: var Redis, key: string): BiggestInt =
  ## Get the length of the value stored in a key. Returns 0 when key doesn't
  ## exist.
  r.sendCommand("STRLEN", key)
  result = r.readInteger()

# Hashes
proc hDel*(r: var Redis, key: string, field: string): bool =
  ## Delete a hash field at `key`. Returns `true` if the field was removed.
  r.sendCommand("HDEL", key, [field])
  result = (r.readInteger()) == 1

proc hDel*(r: var Redis, key: string, fields: seq[string]): BiggestInt =
  ## Delete hash fields at `key`. Returns number of fields removed.
  r.sendCommand("HDEL", key, fields)
  result = r.readInteger()

proc hExists*(r: var Redis, key, field: string): bool =
  ## Determine if a hash field exists.
  r.sendCommand("HEXISTS", key, [field])
  result = (r.readInteger()) == 1

proc hGet*(r: var Redis, key, field: string): string =
  ## Get the value of a hash field
  r.sendCommand("HGET", key, [field])
  result = r.readBulkString()

proc hGetAll*(r: var Redis, key: string): Table[string, string] =
  ## Get all the fields and values in a hash
  r.sendCommand("HGETALL", key)
  result = r.readArrayToTable()

proc hIncrBy*(r: var Redis, key, field: string, incr: int): BiggestInt =
  ## Increment the integer value of a hash field by the given number
  r.sendCommand("HINCRBY", key, [field, $incr])
  result = r.readInteger()

proc hKeys*(r: var Redis, key: string): seq[string] =
  ## Get all the fields in a hash
  r.sendCommand("HKEYS", key)
  result = r.readArray()

proc hLen*(r: var Redis, key: string): BiggestInt =
  ## Get the number of fields in a hash
  r.sendCommand("HLEN", key)
  result = r.readInteger()

proc hMGet*(r: var Redis, key: string, fields: seq[string]): seq[string] =
  ## Get the values of all the given hash fields
  r.sendCommand("HMGET", key, fields)
  result = r.readArray()

proc hMSet*(r: var Redis, key: string, fieldValues: seq[(string, string)]) =
  ## Set multiple hash fields to multiple values
  var args = @[key]
  for field, value in fieldValues.items:
    args.add(field)
    args.add(value)
  r.sendCommand("HMSET", args)
  raiseUnlessOkay r

proc hSet*(r: var Redis, key, field, value: string): BiggestInt =
  ## Set the string value of a hash field
  r.sendCommand("HSET", key, [field, value])
  result = r.readInteger()

proc hSetNX*(r: var Redis, key, field, value: string): BiggestInt =
  ## Set the value of a hash field, only if the field does **not** exist
  r.sendCommand("HSETNX", key, [field, value])
  result = r.readInteger()

proc hVals*(r: var Redis, key: string): seq[string] =
  ## Get all the values in a hash
  r.sendCommand("HVALS", key)
  result = r.readArray()

# Lists

proc bLPop*(r: var Redis, keys: seq[string], timeout: int): seq[string] =
  ## Remove and get the *first* element in a list, or block until
  ## one is available
  r.sendCommand("BLPOP", keys & $timeout)
  result = r.readArray()

proc bRPop*(r: var Redis, keys: seq[string], timeout: int): seq[string] =
  ## Remove and get the *last* element in a list, or block until one
  ## is available.
  r.sendCommand("BRPOP", keys & $timeout)
  result = r.readArray()

proc bRPopLPush*(r: var Redis, source, destination: string,
                 timeout: int): string =
  ## Pop a value from a list, push it to another list and return it; or
  ## block until one is available.
  ##
  ## http://redis.io/commands/brpoplpush
  r.sendCommand("BRPOPLPUSH", source, [destination, $timeout])
  result = r.readBulkString(true) # Multi-Bulk nil allowed.

proc lIndex*(r: var Redis, key: string, index: int): string  =
  ## Get an element from a list by its index
  r.sendCommand("LINDEX", key, [$index])
  result = r.readBulkString()

proc lInsert*(r: var Redis, key: string, before: bool, pivot, value: string): BiggestInt =
  ## Insert an element before or after another element in a list
  var pos = if before: "BEFORE" else: "AFTER"
  r.sendCommand("LINSERT", key, [pos, pivot, value])
  result = r.readInteger()

proc lLen*(r: var Redis, key: string): BiggestInt =
  ## Get the length of a list
  r.sendCommand("LLEN", key)
  result = r.readInteger()

proc lPop*(r: var Redis, key: string): string =
  ## Remove and get the first element in a list
  r.sendCommand("LPOP", key)
  result = r.readBulkString()

proc lPush*(r: var Redis, key, value: string, create = true): BiggestInt =
  ## Prepend a value to a list. Returns the length of the list after the push.
  ## The ``create`` param specifies whether a list should be created if it
  ## doesn't exist at ``key``. More specifically if ``create`` is true, `LPUSH`
  ## will be used, otherwise `LPUSHX`.
  if create:
    r.sendCommand("LPUSH", key, [value])
  else:
    r.sendCommand("LPUSHX", key, [value])

  result = r.readInteger()

proc lPush*(r: var Redis, key: string, values: seq[string], create = true): BiggestInt =
  ## Prepend a value to a list. Returns the length of the list after the push.
  ## The ``create`` param specifies whether a list should be created if it
  ## doesn't exist at ``key``. More specifically if ``create`` is true, `LPUSH`
  ## will be used, otherwise `LPUSHX`.
  if create:
    r.sendCommand("LPUSH", key, values)
  else:
    r.sendCommand("LPUSHX", key, values)

  result = r.readInteger()

proc lRange*(r: var Redis, key: string, start, stop: int): seq[string] =
  ## Get a range of elements from a list. Returns `nil` when `key`
  ## doesn't exist.
  r.sendCommand("LRANGE", key, [$start, $stop])
  result = r.readArray()

proc lRem*(r: var Redis, key: string, value: string, count = 0): BiggestInt =
  ## Remove elements from a list. Returns the number of elements that have been
  ## removed.
  r.sendCommand("LREM", key, [$count, value])
  result = r.readInteger()

proc lSet*(r: var Redis, key: string, index: int, value: string) =
  ## Set the value of an element in a list by its index
  r.sendCommand("LSET", key, [$index, value])
  raiseUnlessOkay r

proc lTrim*(r: var Redis, key: string, start, stop: int)  =
  ## Trim a list to the specified range
  r.sendCommand("LTRIM", key, [$start, $stop])
  raiseUnlessOkay r

proc rPop*(r: var Redis, key: string): string =
  ## Remove and get the last element in a list
  r.sendCommand("RPOP", key)
  result = r.readBulkString()

proc rPopLPush*(r: var Redis, source, destination: string): string =
  ## Remove the last element in a list, append it to another list and return it
  r.sendCommand("RPOPLPUSH", source, [destination])
  result = r.readBulkString()

proc rPush*(r: var Redis, key, value: string,
            create = true): BiggestInt =
  ## Append a value to a list. Returns the length of the list after the push.
  ## The ``create`` param specifies whether a list should be created if it
  ## doesn't exist at ``key``. More specifically if ``create`` is true, `RPUSH`
  ## will be used, otherwise `RPUSHX`.
  if create:
    r.sendCommand("RPUSH", key, [value])
  else:
    r.sendCommand("RPUSHX", key, [value])

  result = r.readInteger()

proc rPush*(r: var Redis, key: string, values: seq[string],
             create = true): BiggestInt =
  ## Append a value to a list. Returns the length of the list after the push.
  ## The ``create`` param specifies whether a list should be created if it
  ## doesn't exist at ``key``. More specifically if ``create`` is true, `RPUSH`
  ## will be used, otherwise `RPUSHX`.
  if create:
    r.sendCommand("RPUSH", key, values)
  else:
    r.sendCommand("RPUSHX", key, values)

  result = r.readInteger()

proc sort*(r: var Redis, key: string, by="", offset = -1, count = -1,
           desc=false, alpha=false, get: seq[string] = @[]): seq[string] =
  var args: seq[string]
  if by.len > 0:
    args.add("BY")
    args.add(by)
  if offset > -1 and count > -1:
    args.add("LIMIT")
    args.add($offset)
    args.add($count)
  if get.len > 0:
    for k in get:
      args.add("GET")
      args.add(k)
  if desc:
    args.add("DESC")
  if alpha:
    args.add("ALPHA")

  r.sendCommand("SORT", key, args)
  result = r.readArray()

# Sets

proc sadd*(r: var Redis, key: string, member: string): BiggestInt =
  ## Add a member to a set
  r.sendCommand("SADD", key, [member])
  result = r.readInteger()

proc sadd*(r: var Redis, key: string, members: seq[string]): BiggestInt =
  ## Add a member to a set
  r.sendCommand("SADD", key, members)
  result = r.readInteger()

proc scard*(r: var Redis, key: string): BiggestInt =
  ## Get the number of members in a set
  r.sendCommand("SCARD", key)
  result = r.readInteger()

proc sdiff*(r: var Redis, keys: seq[string]): seq[string] =
  ## Subtract multiple sets
  r.sendCommand("SDIFF", keys)
  result = r.readArray()

proc sdiffstore*(r: var Redis, destination: string,
                keys: seq[string]): BiggestInt =
  ## Subtract multiple sets and store the resulting set in a key
  r.sendCommand("SDIFFSTORE", destination, keys)
  result = r.readInteger()

proc sinter*(r: var Redis, keys: seq[string]): seq[string] =
  ## Intersect multiple sets
  r.sendCommand("SINTER", keys)
  result = r.readArray()

proc sinterstore*(r: var Redis, destination: string,
                 keys: seq[string]): BiggestInt =
  ## Intersect multiple sets and store the resulting set in a key
  r.sendCommand("SINTERSTORE", destination, keys)
  result = r.readInteger()

proc sismember*(r: var Redis, key: string, member: string): bool =
  ## Determine if a given value is a member of a set
  r.sendCommand("SISMEMBER", key, [member])
  result = (r.readInteger()) == 1

proc smembers*(r: var Redis, key: string): seq[string] =
  ## Get all the members in a set
  r.sendCommand("SMEMBERS", key)
  result = r.readArray()

proc smove*(r: var Redis, source: string, destination: string,
           member: string): BiggestInt =
  ## Move a member from one set to another
  r.sendCommand("SMOVE", source, [destination, member])
  result = r.readInteger()

proc spop*(r: var Redis, key: string): string =
  ## Remove and return a random member from a set
  r.sendCommand("SPOP", key)
  result = r.readBulkString()

proc srandmember*(r: var Redis, key: string): string =
  ## Get a random member from a set
  r.sendCommand("SRANDMEMBER", key)
  result = r.readBulkString()

proc srandmember*(r: var Redis, key: string; count: int): seq[string] =
  ## Get random members from a set
  r.sendCommand("SRANDMEMBER", key, [$count])
  result = r.readArray()

proc srem*(r: var Redis, key: string, member: string): BiggestInt =
  ## Remove a member from a set
  r.sendCommand("SREM", key, [member])
  result = r.readInteger()

proc sunion*(r: var Redis, keys: seq[string]): seq[string] =
  ## Add multiple sets
  r.sendCommand("SUNION", keys)
  result = r.readArray()

proc sunionstore*(r: var Redis, destination: string,
                 key: seq[string]): BiggestInt =
  ## Add multiple sets and store the resulting set in a key
  r.sendCommand("SUNIONSTORE", destination, key)
  result = r.readInteger()

# Sorted sets

proc readArrayWithScores*(r: var Redis): seq[(string, float)] =
  let results = r.readArray()
  if results.len > 0:
    for index in 0..<(results.len div 2):
      template score: string {.dirty.} = results[(index * 2) + 1]
      when not defined(release):
        # guard something crazy coming out of redis
        if score in ["", "inf", "Inf", "-inf", "-Inf", "nan", "NaN"]:
          raise Defect.newException "unexpected score: " & repr(score)
      let value =
        case score
        of "-inf", "-Inf":
          -Inf
        of "inf", "Inf":
          Inf
        of "nan", "NaN":
          NaN
        else:
          try:
            parseFloat score
          except ValueError:
            NaN
      result.add (results[index * 2], value)

proc zpopmin*(r: var Redis; key: string; count=1): seq[(string, float)] =
  ## Remove `count` lowest-scored members of a sorted set `key`.
  ## Returns the removed members and their scores.
  r.sendCommand("ZPOPMIN", key, $count)
  result = r.readArrayWithScores()

proc zpopmax*(r: var Redis; key: string; count=1): seq[(string, float)] =
  ## Remove `count` highest-scored members of a sorted set `key`.
  ## Returns the removed members.
  r.sendCommand("ZPOPMAX", key, $count)
  result = r.readArrayWithScores()

proc zrandmember*(r: var Redis; key: string): string =
  ## Retrieve a single random member of a sorted set `key`.
  r.sendCommand("ZRANDMEMBER", key)
  result = r.readBulkString(allowMBNil=false)

proc zrandmembers*(r: var Redis; key: string; count=1): seq[string] =
  ## Retrieve `count` distinct random members of a sorted set `key`.
  ## Use a negative `count` to relax the distinct requirement.
  r.sendCommand("ZRANDMEMBER", key, $count)
  result = r.readArray()

proc zrandmembers*(r: var Redis; key: string; withScores: bool; count=1): seq[(string, float)] =
  ## Returns `count` distinct random members.
  ## Use a negative `count` to relax the distinct requirement.
  ## If `withScores` is true, populate the floats with their scores.
  if withScores:
    r.sendCommand("ZRANDOMMEMBER", [key, $count, "WITHSCORES"])
    result = r.readArrayWithScores()
  else:
    for member in r.zrandmembers(key, count).items:
      result.add (member, NaN)

proc zadd*(r: var Redis; key: string; score: float; member: string;
           nan = "-inf"): BiggestInt =
  ## Add a member to a sorted set, or update its score if it already
  ## exists.
  ## Provide `nan` as a substitute value for NaN scores.
  ## Returns the number of added members.
  let score =
    if score.isNaN:
      nan
    else:
      $score
  r.sendCommand("ZADD", key, [score, member])
  result = r.readInteger()

proc zadd*(r: var Redis; key: string;
           members: seq[(string, float)]; nan = "-inf"): BiggestInt =
  ## Add members to a sorted set, or update their scores if they already exist.
  ## Provide `nan` as a substitute value for NaN floats.
  ## Returns the number of added members.
  if members.len == 0:
    return 0
  var values = newSeqOfCap[string](members.len * 2)
  for member, score in members.items:
    values.add:
      if score.isNaN:
        nan
      else:
        $score
    values.add member
  r.sendCommand("ZADD", key, values)
  result = r.readInteger()

proc zcard*(r: var Redis, key: string): BiggestInt =
  ## Get the number of members in a sorted set
  r.sendCommand("ZCARD", key)
  result = r.readInteger()

proc zcount*(r: var Redis, key, min, max: string): BiggestInt =
  ## Count the members in a sorted set with scores within the given values
  r.sendCommand("ZCOUNT", key, [min, max])
  result = r.readInteger()

proc zincrby*(r: var Redis, key: string, increment: float,
             member: string): string  =
  ## Increment the score of a member in a sorted set
  r.sendCommand("ZINCRBY", key, [$increment, member])
  result = r.readBulkString()

proc zinterstore*(r: var Redis, destination: string,
                  keyWeights: seq[(string, float)],
                  aggregate = ""): BiggestInt =
  ## Intersect multiple sorted sets and store the resulting sorted set in
  ## a new key
  var args = @[destination, $keyWeights.len]
  var weights: seq[string]

  for key, weight in keyWeights.items:
    args.add(key)
    weights.add($weight)

  args.add("WEIGHTS")
  args.add(weights)

  if aggregate.len != 0:
    args.add("AGGREGATE")
    args.add(aggregate)

  r.sendCommand("ZINTERSTORE", args)
  result = r.readInteger()

proc zinterstore*(r: var Redis, destination: string,
                 keys: seq[string], weights: seq[string] = @[],
                 aggregate = ""): BiggestInt =
  ## Intersect multiple sorted sets and store the resulting sorted set in
  ## a new key
  var args = @[destination, $keys.len]
  args.add(keys)

  if weights.len != 0:
    args.add("WEIGHTS")
    for i in weights:
      args.add(i)

  if aggregate.len != 0:
    args.add("AGGREGATE")
    args.add(aggregate)

  r.sendCommand("ZINTERSTORE", args)

  result = r.readInteger()

proc zrange*(r: var Redis, key: string, start, stop: int): seq[string] =
  ## Return a range of members in a sorted set, by index
  r.sendCommand("ZRANGE", key, [$start, $stop])
  result = r.readArray()

proc zrange*(r: var Redis, key: string, start, stop: int,
            withScores: bool): seq[(string, float)] =
  ## Return a range of members in a sorted set, by index.
  ## When `withScores` is true, scores are also populated.
  if withScores:
    r.sendCommand("ZRANGE", key, [$start, $stop, "WITHSCORES"])
    result = r.readArrayWithScores()
  else:
    let members = r.zrange(key, start, stop)
    result = newSeq[(string, float)](members.len)
    for index, member in members.pairs:
      result[index] = (member, NaN)

proc zrangebyscore*(r: var Redis, key, min, max: string,
                    withScores = false, limit = false, limitOffset = 0,
                    limitCount = 0): seq[string] =
  ## Return a range of members in a sorted set, by score
  var args = @[key, $min, $max]

  if withScores: args.add("WITHSCORES")
  if limit:
    args.add("LIMIT")
    args.add($limitOffset)
    args.add($limitCount)

  r.sendCommand("ZRANGEBYSCORE", args)
  result = r.readArray()

proc zrangebylex*(r: var Redis, key, start, stop: string,
                  limit = false, limitOffset = 0,
                  limitCount = 0): seq[string] =
  ## Return a range of members in a sorted set, ordered lexicographically
  var args = @[key, start, stop]

  if limit:
    args.add("LIMIT")
    args.add($limitOffset)
    args.add($limitCount)

  r.sendCommand("ZRANGEBYLEX", args)
  result = r.readArray()

proc zrank*(r: var Redis, key, member: string): BiggestInt =
  ## Determine the index of a member in a sorted set
  r.sendCommand("ZRANK", key, [member])
  try:
    result = readInteger r
  except ReplyError:
    result = -1

proc zrem*(r: var Redis, key: string, member: string): BiggestInt =
  ## Remove a member from a sorted set
  r.sendCommand("ZREM", key, [member])
  result = r.readInteger()

proc zrem*(r: var Redis, key: string, members: seq[string]): BiggestInt =
  ## Remove members from a sorted set
  r.sendCommand("ZREM", key, members)
  result = r.readInteger()

proc zremrangebyrank*(r: var Redis, key, start, stop: string): BiggestInt =
  ## Remove all members in a sorted set within the given indexes
  r.sendCommand("ZREMRANGEBYRANK", key, [start, stop])
  result = r.readInteger()

proc zremrangebyscore*(r: var Redis, key, min, max: string): BiggestInt =
  ## Remove all members in a sorted set within the given scores
  r.sendCommand("ZREMRANGEBYSCORE", key, [min, max])
  result = r.readInteger()

proc zrevrange*(r: var Redis, key: string, start: string, stop: string,
               withScores = false): seq[string] =
  ## Return a range of members in a sorted set, by index,
  ## with scores ordered from high to low
  if withScores:
    r.sendCommand("ZREVRANGE", key, [start, stop, "WITHSCORES"])
  else:
    r.sendCommand("ZREVRANGE", key, [start, stop])

  result = r.readArray()

proc zrevrangebyscore*(r: var Redis, key, min, max: string,
                       withScores = false, limit = false, limitOffset = 0,
                       limitCount = 0): seq[string] =
  ## Return a range of members in a sorted set, by score, with
  ## scores ordered from high to low
  var args = @[key, min, max]

  if withScores: args.add("WITHSCORES")
  if limit:
    args.add("LIMIT")
    args.add($limitOffset)
    args.add($limitCount)

  r.sendCommand("ZREVRANGEBYSCORE", args)
  result = r.readArray()

proc zrevrank*(r: var Redis, key, member: string): string =
  ## Determine the index of a member in a sorted set, with
  ## scores ordered from high to low
  r.sendCommand("ZREVRANK", key, [member])
  try:
    result = $(r.readInteger())
  except ReplyError:
    result = redisNil

proc zscore*(r: var Redis, key, member: string): float =
  ## Get the score associated with the given member in a sorted set
  r.sendCommand("ZSCORE", key, [member])
  let score = r.readBulkString()
  if score.len > 0 and score != redisNil:
    return parseFloat(score)

proc zunionstore*(r: var Redis, destination: string,
                  keyWeights: seq[(string, float)],
                  aggregate = ""): BiggestInt =
  ## Add multiple sorted sets and store the resulting sorted set in a new key
  var args = @[destination, $keyWeights.len]
  var weights: seq[string]

  for key, weight in keyWeights.items:
    args.add(key)
    weights.add($weight)

  args.add("WEIGHTS")
  args.add(weights)

  if aggregate.len != 0:
    args.add("AGGREGATE")
    args.add(aggregate)

  r.sendCommand("ZUNIONSTORE", args)
  result = r.readInteger()

proc zunionstore*(r: var Redis, destination: string,
                 keys: seq[string], weights: seq[string] = @[],
                 aggregate = ""): BiggestInt =
  ## Add multiple sorted sets and store the resulting sorted set in a new key
  var args = @[destination, $keys.len]
  args.add(keys)

  if weights.len != 0:
    args.add("WEIGHTS")
    for i in weights:
      args.add(i)

  if aggregate.len != 0:
    args.add("AGGREGATE")
    args.add(aggregate)

  r.sendCommand("ZUNIONSTORE", args)
  result = r.readInteger()

# HyperLogLog

proc pfadd*(r: var Redis, key: string, elements: seq[string]): BiggestInt =
  ## Add variable number of elements into special 'HyperLogLog' set type
  r.sendCommand("PFADD", key, elements)
  result = r.readInteger()

proc pfcount*(r: var Redis, key: string): BiggestInt =
  ## Count approximate number of elements in 'HyperLogLog'
  r.sendCommand("PFCOUNT", key)
  result = r.readInteger()

proc pfcount*(r: var Redis, keys: seq[string]): BiggestInt =
  ## Count approximate number of elements in 'HyperLogLog'
  r.sendCommand("PFCOUNT", keys)
  result = r.readInteger()

proc pfmerge*(r: var Redis, destination: string, sources: seq[string]) =
  ## Merge several source HyperLogLog's into one specified by destKey
  r.sendCommand("PFMERGE", destination, sources)
  raiseUnlessOkay r

# Pub/Sub

# proc psubscribe*(r: var Redis, pattern: openarray[string]): ???? =
#   ## Listen for messages published to channels matching the given patterns
#   r.socket.send("PSUBSCRIBE $#\r\n" % pattern)
#   return ???

proc publish*(r: var Redis, channel: string, message: string): BiggestInt =
  ## Post a message to a channel
  r.sendCommand("PUBLISH", channel, [message])
  result = r.readInteger()

# proc punsubscribe*(r: var Redis, [pattern: openarray[string], : string): ???? =
#   ## Stop listening for messages posted to channels matching the given patterns
#   r.socket.send("PUNSUBSCRIBE $# $#\r\n" % [[pattern.join(), ])
#   return ???


proc subscribe*(r: var Redis, channel: string) =
  ## Listen for messages published to the given channels
  r.sendCommand("SUBSCRIBE", [channel])
  discard r.readNext()

# proc unsubscribe*(r: var Redis, [channel: openarray[string], : string): ???? =
#   ## Stop listening for messages posted to the given channels
#   r.socket.send("UNSUBSCRIBE $# $#\r\n" % [[channel.join(), ])
#   return ???

proc nextMessage*(r: var Redis): tuple[channel: string, message: string] =
  let msg = r.readNext()
  assert msg[0] == "message"
  result = (channel: msg[1], message: msg[2])

# Transactions

proc discardMulti*(r: var Redis) =
  ## Discard all commands issued after MULTI
  r.sendCommand("DISCARD")
  raiseUnlessOkay r

proc exec*(r: var Redis): seq[string] =
  ## Execute all commands issued after MULTI
  r.sendCommand("EXEC")
  r.pipeline.enabled = false
  # Will reply with +OK for MULTI/EXEC and +QUEUED for every command
  # between, then with the results
  result = r.flushPipeline(true)

proc multi*(r: var Redis) =
  ## Mark the start of a transaction block
  r.startPipelining()
  r.sendCommand("MULTI")
  raiseUnlessOkay r

proc unwatch*(r: var Redis) =
  ## Forget about all watched keys
  r.sendCommand("UNWATCH")
  raiseUnlessOkay r

proc watch*(r: var Redis, keys: seq[string]) =
  ## Watch the given keys to determine execution of the MULTI/EXEC block
  r.sendCommand("WATCH", keys)
  raiseUnlessOkay r

template watchTimeout*(r: var Redis; keys: seq[string]; timeout: float; body: untyped) =
  let done = epochTime() + timeout
  var first = true
  while epochTime() < done or first:
    first = false
    try:
      r.watch(keys)
      body
      break
    except WatchError:
      discard

# Connection

proc auth*(r: var Redis, password: string) =
  ## Authenticate to the server
  r.sendCommand("AUTH", password)
  raiseUnlessOkay r

proc echoServ*(r: var Redis, message: string): string =
  ## Echo the given string
  r.sendCommand("ECHO", message)
  result = r.readBulkString()

proc ping*(r: var Redis): RedisStatus =
  ## Ping the server
  r.sendCommand("PING")
  result = r.readStatus()

proc quit*(r: var Redis) =
  ## Close the connection
  if r.isConnected:
    r.sendCommand("QUIT")
    raiseUnlessOkay r
    close r

proc select*(r: var Redis, index: int): RedisStatus =
  ## Change the selected database for the current connection
  r.sendCommand("SELECT", $index)
  result = r.readStatus()

# Server

proc bgrewriteaof*(r: var Redis) =
  ## Asynchronously rewrite the append-only file
  r.sendCommand("BGREWRITEAOF")
  raiseUnlessOkay r

proc bgsave*(r: var Redis) =
  ## Asynchronously save the dataset to disk
  r.sendCommand("BGSAVE")
  raiseUnlessOkay r

proc configGet*(r: var Redis, parameter: string): seq[string] =
  ## Get the value of a configuration parameter
  r.sendCommand("CONFIG", "GET", [parameter])
  result = r.readArray()

proc configSet*(r: var Redis, parameter: string, value: string) =
  ## Set a configuration parameter to the given value
  r.sendCommand("CONFIG", "SET", [parameter, value])
  raiseUnlessOkay r

proc configResetStat*(r: var Redis) =
  ## Reset the stats returned by INFO
  r.sendCommand("CONFIG", "RESETSTAT")
  raiseUnlessOkay r

proc dbsize*(r: var Redis): BiggestInt =
  ## Return the number of keys in the selected database
  r.sendCommand("DBSIZE")
  result = r.readInteger()

proc debugObject*(r: var Redis, key: string): RedisStatus =
  ## Get debugging information about a key
  r.sendCommand("DEBUG", "OBJECT", [key])
  result = r.readStatus()

proc debugSegfault*(r: var Redis) =
  ## Make the server crash
  r.sendCommand("DEBUG", "SEGFAULT")

proc flushall*(r: var Redis): RedisStatus =
  ## Remove all keys from all databases
  r.sendCommand("FLUSHALL")
  raiseUnlessOkay r

proc flushdb*(r: var Redis): RedisStatus =
  ## Remove all keys from the current database
  r.sendCommand("FLUSHDB")
  raiseUnlessOkay r

proc info*(r: var Redis): string =
  ## Get information and statistics about the server
  r.sendCommand("INFO")
  result = r.readBulkString()

proc infoTable*(r: var Redis): Table[string, string] =
  ## Get information and statistics about the server as a table
  var info = r.info()
  for line in info.split("\n"):
    if line.len == 1 or line[0] == '#': continue
    let keyval = line.split(":")
    result[keyval[0]] = keyval[1].strip(chars={'\c'})

proc lastsave*(r: var Redis): BiggestInt =
  ## Get the UNIX time stamp of the last successful save to disk
  r.sendCommand("LASTSAVE")
  result = r.readInteger()

proc save*(r: var Redis) =
  ## Synchronously save the dataset to disk
  r.sendCommand("SAVE")
  raiseUnlessOkay r

proc shutdown*(r: var Redis) =
  ## Synchronously save the dataset to disk and then shut down the server
  r.sendCommand("SHUTDOWN")
  safeSocket r:
    let s = recvLine(r.socket)
    if s.len != 0:
      raise RedisError.newException s

proc slaveof*(r: var Redis, host: string, port: string) =
  ## Make the server a slave of another instance, or promote it as master
  r.sendCommand("SLAVEOF", host, [port])
  raiseUnlessOkay r
