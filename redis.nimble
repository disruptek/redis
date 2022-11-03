version = "0.0.5"
author = "disruptek"
description = "redis rewrite, right"
license = "MIT"

when not defined(release):
  requires "https://github.com/disruptek/balls >= 3.0.0 & < 4.0.0"

task test, "run tests for ci":
  when defined(windows):
    exec "balls.cmd"
  else:
    exec "balls"
