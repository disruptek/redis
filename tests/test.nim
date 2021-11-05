import std/tables

import balls

import redis

type
  SendMode = enum normal, pipelined, multiple

proc someTests(r: var Redis; how: SendMode): seq[string] =
  var list: seq[string]

  if how == pipelined:
    startPipelining r
  elif how == multiple:
    multi r

  r.setk("nim:test", "Testing something.")
  r.setk("nim:utf8", "こんにちは")
  r.setk("nim:esc", "\\ths ągt\\")
  r.setk("nim:int", "1")
  list.add(r.get("nim:esc"))
  list.add($(r.incr("nim:int")))
  list.add(r.get("nim:int"))
  list.add(r.get("nim:utf8"))
  list.add($(r.hSet("test1", "name", "A Test")))
  var res = r.hGetAll("test1")
  for k, v in res:
    list.add(k)
    list.add(v)
  list.add(r.get("invalid_key"))
  list.add($(r.lPush("mylist","itema")))
  list.add($(r.lPush("mylist","itemb")))
  r.lTrim("mylist",0,1)
  var p = r.lRange("mylist", 0, -1)

  for i in p:
    if i.len > 0:
      list.add(i)

  list.add($r.debugObject("mylist"))

  r.configSet("timeout", "299")
  var g = r.configGet("timeout")
  for i in g:
    list.add(i)

  list.add(r.echoServ("BLAH"))

  result =
    case how
    of normal:
      list
    of pipelined:
      flushPipeline r
    of multiple:
      exec r

var r: Redis

suite "pipelining":
  r = openRedis()

  ## Test with no pipelining
  let listNormal = r.someTests normal

  ## Test with pipelining enabled
  let listPipelined = r.someTests pipelined
  check listNormal == listPipelined

  ## Test with multi/exec() (automatic pipelining)
  let listMulti = r.someTests multiple
  check listNormal == listMulti

  echo "Normal: ", listNormal
  echo "Pipelined: ", listPipelined
  echo "Multi: ", listMulti

suite "redis":
  r = openRedis "localhost"
  check "OK" == $r.select 13
  let keys = r.keys("*")
  if keys.len != 0:
    echo "Don't want to mess up an existing DB."
    quit 0

  test "simple set and get":
    const expected = "Hello, World!"

    r.setk("redisTests:simpleSetAndGet", expected)
    let actual = r.get("redisTests:simpleSetAndGet")

    check actual == expected

  test "increment key by one":
    const expected = 3

    r.setk("redisTests:incrementKeyByOne", "2")
    let actual = r.incr("redisTests:incrementKeyByOne")

    check actual == expected

  test "increment key by five":
    const expected = 10

    r.setk("redisTests:incrementKeyByFive", "5")
    let actual = r.incrBy("redisTests:incrementKeyByFive", 5)

    check actual == expected

  test "decrement key by one":
    const expected = 2

    r.setk("redisTest:decrementKeyByOne", "3")
    let actual = r.decr("redisTest:decrementKeyByOne")

    check actual == expected

  test "decrement key by three":
    const expected = 7

    r.setk("redisTest:decrementKeyByThree", "10")
    let actual = r.decrBy("redisTest:decrementKeyByThree", 3)

    check actual == expected

  test "append string to key":
    const expected = "hello world"

    r.setk("redisTest:appendStringToKey", "hello")
    let keyLength = r.append("redisTest:appendStringToKey", " world")

    check keyLength == len(expected)
    check r.get("redisTest:appendStringToKey") == expected

  test "check key exists":
    r.setk("redisTest:checkKeyExists", "foo")
    check r.exists("redisTest:checkKeyExists") == true

  test "delete key":
    r.setk("redisTest:deleteKey", "bar")
    check r.exists("redisTest:deleteKey") == true

    check r.del(@["redisTest:deleteKey"]) == 1
    check r.exists("redisTest:deleteKey") == false

  test "rename key":
    const expected = "42"

    r.setk("redisTest:renameKey", expected)
    discard r.rename("redisTest:renameKey", "redisTest:meaningOfLife")

    check r.exists("redisTest:renameKey") == false
    check r.get("redisTest:meaningOfLife") == expected

  test "get key length":
    const expected = 5

    r.setk("redisTest:getKeyLength", "hello")
    let actual = r.strlen("redisTest:getKeyLength")

    check actual == expected

  test "push entries to list":
    for i in 1..5:
      check r.lPush("redisTest:pushEntriesToList", $i) == i

    check r.llen("redisTest:pushEntriesToList") == 5

  test "pfcount supports single key and multiple keys":
    discard r.pfadd("redisTest:pfcount1", @["foo"])
    check r.pfcount("redisTest:pfcount1") == 1

    discard r.pfadd("redisTest:pfcount2", @["bar"])
    check r.pfcount(@["redisTest:pfcount1", "redisTest:pfcount2"]) == 2

  # TODO: Ideally tests for all other procedures, will add these in the future

  # delete all keys in the DB at the end of the tests
  discard r.flushdb()
  quit r
