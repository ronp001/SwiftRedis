# SwiftRedis

A preliminary version of an asynchronous iOS Redis client written in Swift 2.

Currently supports the following:
* Asynchronous operation
* Has a "generic" mechanism that allows sending any Redis command that returns a result
* Utility functions to send GET, SET, AUTH commands
* Support for PubSub commands: PUBLISH, SUBSCRIBE
* Mechanism to extend with more commands
* Provides a RedisResponse class which encapsulates possible responses from Redis:  String, Int, Data (BulkString), Error, Array

Blatanly missing:
* Error checking still needs work.
* Source code definitely needs more documentation
* SSL connectivity not tested.
* A single RedisInterface object can only subscribe to a single channel (workaround: use two RedisInterface objects)


## Usage

Add the SwiftRedis source files to your project.  Create a RedisInterface object, call connect() and then issue Redis commands.


## Simple example

```swift
let redis = RedisInterface(host: <host-address String>, port: <port Int>, auth: <auth String>)

// Queue a request to initiate a connection.
// Once a connection is established, an AUTH command will be issued with the auth parameters specified above.
redis.connect()

// Queue a request to set a value for a key in the Redis database.  This command will only
// execute after the connection is established and authenticated.
redis.setValueForKey("some:key", stringValue: "a value", completionHandler: { success, cmd in
    // this completion handler will be executed after the SET command returns
    if success {
        print("value stored successfully")
    } else {
        print("value was not stored")
    }
})

// Queue a request to get the value of a key in the Redis database.  This command will only
// execute after the previous command is complete.
redis.getDataForKey("some:key", completionHandler: { success, key, data, cmd in
    if success {
        print("the stored data for \(key) is \(data!.stringVal)")
    } else {
        print("could not get value for \(key)")
    }
})

// Queue a QUIT command (the connection will close when the QUIT command returns)
redis.quit({success, cmd in
    print("QUIT command completed")
})
```


    


## For more info

To understand high-level usage: check the Unit Tests for usage of the  RedisInterface() class.

To understand internals and learn how to extend this:  examine the Unit Tests for the lower-level classes (RedisConnection, RedisCommand, RedisParser, etc.)

Note that in order to run the unit tests, you'll need to specifiy host/port/auth details in the ConnectionParams class.


