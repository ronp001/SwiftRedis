//
//  SwiftRedisTests.swift
//  SwiftRedisTests
//
//  Created by Ron Perry on 11/9/15.
//  Copyright © 2015 Ron Perry. All rights reserved.
//
import XCTest
@testable import SwiftRedis


class RedisInterfaceTests: XCTestCase
{
    func testReadmeExample()
    {
        
        let redis = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
//        let redis = RedisInterface(host: <host-address String>, port: <port Int>, auth: <auth String>)
        
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
        redis.getValueForKey("some:key", completionHandler: { success, key, data, cmd in
            if success {
                print("the stored data for \(key) is \(data!.stringVal)")
            } else {
                print("could not get value for \(key)")
            }
        })
        
        // Queue a QUIT command (the connection will close when the QUIT command returns)
        var quitComplete: Bool = false
        let doneExpectation = expectation(description: "done")
        redis.quit({success, cmd in
            quitComplete = true
            doneExpectation.fulfill()
        })
        
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssert(quitComplete)
        })
    }
    
    
    func testSetAndGet()
    {
        let r = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
        
        r.connect()
        
        let data1 = "hi".data(using: String.Encoding.utf8)!
        let data2 = "hello, world".data(using: String.Encoding.utf8)!
        
        
        // store data1
        let storedExpectation1 = expectation(description: "testkey1 stored")
        r.setDataForKey("testkey1", data: data1, completionHandler: { success, cmd in
            XCTAssertTrue(success, "expecting success storing data for testkey1")
            storedExpectation1.fulfill()
        })
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "expecting operation to succeed")
        })
        
        // retrieve data1
        let storedExpectation2 = expectation(description: "testkey1 retrieved")
        r.getDataForKey("testkey1", completionHandler: { success, key, data, cmd in
            storedExpectation2.fulfill()
            XCTAssertTrue(success)
            XCTAssert(key == "testkey1")
            XCTAssert(data! == RedisResponse(dataVal: data1))
        })
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "expecting operation to succeed")
        })
        
        
        // store data2
        let storedExpectation3 = expectation(description: "testkey2 stored")
        r.setDataForKey("testkey2", data: data2, completionHandler: { success, cmd in
            XCTAssertTrue(success, "could not store testkey2")
            storedExpectation3.fulfill()
        })
        waitForExpectations(timeout: 5, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
        // retrieve data2
        let storedExpectation4 = expectation(description: "testkey2 stored")
        r.getDataForKey("testkey2", completionHandler: { success, key, data, cmd in
            storedExpectation4.fulfill()
            XCTAssertTrue(success)
            XCTAssert(key == "testkey2")
            XCTAssert(data! == RedisResponse(dataVal: data2))
        })
        waitForExpectations(timeout: 5, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
        
        let storedExpectation5 = expectation(description: "quit complete")
        r.quit({ success in
            storedExpectation5.fulfill()
        })
        waitForExpectations(timeout: 5, handler: { error in
            XCTAssertNil(error, "Error")
        })
    }
    
    
    func testSkipPendingCommandsAndQuit()
    {
        let r = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
        
        r.connect()  // this stores an AUTH command in the queue.  since we are running in a single thread,
                     // the command will not be sent before this routine reaches "waitForExpectation"

        let storedExpectation = expectation(description: "a handler was called")

        
        // queue a command that we do not expect will execute
        r.setValueForKey("testkey1", stringValue: "a value", completionHandler: { success, cmd in
            XCTAssertFalse(true, "not expecting this handler to be called, because the call to testSkipPendingCommandsAndQuit() should have removed the command from the queue")
            storedExpectation.fulfill()
        })

        var quitHandlerCalled = false
        
        r.skipPendingCommandsAndQuit({ success, cmd in
            XCTAssertTrue(success == true, "expecting quit handler to succeed")
            quitHandlerCalled = true
            storedExpectation.fulfill()
        })
        
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "expecting operation to succeed")
            XCTAssertTrue(quitHandlerCalled, "expecting quit handler to have been called")
        })
        
    }
    
    func testPubSub()
    {
        let riPublish = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
        riPublish.connect()
        
        let riSubscribe = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
        riSubscribe.connect()
        
        let expectingSubscribeToReturn1 = expectation(description: "subscribe operation returned once")
        var expectingSubscribeToReturn2: XCTestExpectation? = nil
        var expectingSubscribeToReturn3: XCTestExpectation? = nil
        var subscribeReturnCount = 0
        
        // subscribe to channel "testchannel"
        // important assumption:  no one else is subscribed to this channel!!!
        riSubscribe.subscribe("testchannel", completionHandler: { success, channel, data, cmd in
            
            // this completion handler should be called several times.
            // the first time: to acknowledge that the subscribe operation was registered
            // the next two times:  in response to publish operations
            
            switch subscribeReturnCount {
            case 0:
                XCTAssertTrue(success)
                XCTAssert(channel == "testchannel")
                XCTAssert(data! == RedisResponse(arrayVal: [
                    RedisResponse(dataVal: "subscribe".data(using: String.Encoding.utf8)),
                    RedisResponse(dataVal: "testchannel".data(using: String.Encoding.utf8)),
                    RedisResponse(intVal: 1)
                    ]))
                XCTAssertNotNil(expectingSubscribeToReturn1)
                expectingSubscribeToReturn1.fulfill()
                
                subscribeReturnCount += 1
                
            case 1:
                XCTAssertTrue(success)
                XCTAssert(channel == "testchannel")
                XCTAssert(data! == RedisResponse(arrayVal: [
                    RedisResponse(dataVal: "message".data(using: String.Encoding.utf8)),
                    RedisResponse(dataVal: "testchannel".data(using: String.Encoding.utf8)),
                    RedisResponse(dataVal: "publish op 1".data(using: String.Encoding.utf8)),
                    ]))
                
                XCTAssertNotNil(expectingSubscribeToReturn2)
                expectingSubscribeToReturn2!.fulfill()
                subscribeReturnCount += 1
                
            case 2:
                XCTAssertTrue(success)
                XCTAssert(channel == "testchannel")
                XCTAssert(data! == RedisResponse(arrayVal: [
                    RedisResponse(dataVal: "message".data(using: String.Encoding.utf8)),
                    RedisResponse(dataVal: "testchannel".data(using: String.Encoding.utf8)),
                    RedisResponse(dataVal: "publish op 2".data(using: String.Encoding.utf8)),
                    ]))
                
                XCTAssertNotNil(expectingSubscribeToReturn3)
                expectingSubscribeToReturn3!.fulfill()
                subscribeReturnCount+=1
                
            default:
                XCTAssert(false)
            }
        })
        
        // wait for the subscribe operation to complete
        waitForExpectations(timeout: 1, handler: { error in
            XCTAssertNil(error, "expecting operation to succeed")
        })
        
        XCTAssertEqual(subscribeReturnCount, 1)
        // -----
        
        // publish something to the test channel
        expectingSubscribeToReturn2 = expectation(description: "subscribe operation returned twice")
        let expectingPublishToComplete1 = expectation(description: "publish operation 1 completed")
        riPublish.publish("testchannel", value: "publish op 1", completionHandler: { success, key, data, cmd in
            expectingPublishToComplete1.fulfill()
        })
        
        // wait for both the publish to complete, and the subscribe to return the 2nd time
        waitForExpectations(timeout: 1, handler: { error in
            XCTAssertNil(error, "expecting publish 1 to complete, and subscribe to return 2nd time")
        })
        
        XCTAssertEqual(subscribeReturnCount, 2)
        
        // -----
        
        // publish something else to the test channel
        expectingSubscribeToReturn3 = expectation(description: "subscribe operation returned third time")
        let expectingPublishToComplete2 = expectation(description: "publish operation 2 completed")
        riPublish.publish("testchannel", value: "publish op 2", completionHandler: { success, key, data, cmd in
            expectingPublishToComplete2.fulfill()
        })
        
        // wait for both the publish to complete, and the subscribe to return the 2nd time
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "expecting publish 2 to complete, and subscribe to return 3nd time")
        })
        
        XCTAssertEqual(subscribeReturnCount, 3)
        
        
    }
}




class RedisConnectionTests: XCTestCase {
    
    let authCmd = RedisCommand.Auth(ConnectionParams.auth, handler: nil)
    
    func testAuthentication()
    {
        let r = RedisConnection(serverAddress: ConnectionParams.serverAddress, serverPort: ConnectionParams.serverPort)
        r.connect()
        
        // test that it works once
        let storedExpectation1 = expectation(description: "set command handler activated")
        let cmd = RedisCommand.Auth(ConnectionParams.auth, handler: { success, cmd in
            XCTAssertTrue(success, "auth command expected to succeed")
            XCTAssert(cmd.response! == RedisResponse(stringVal: "OK"), "expecting response from Redis to be OK")
            storedExpectation1.fulfill()
        })
        
        r.setPendingCommand(cmd)
        
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
    }
    
    func testDisconnect()
    {
        let r = RedisConnection(serverAddress: ConnectionParams.serverAddress, serverPort: ConnectionParams.serverPort)
        r.connect()
        
        // test that it works once
        let storedExpectation1 = expectation(description: "set command handler activated")
        let cmd = RedisCommand.Auth(ConnectionParams.auth, handler: { success, cmd in
            XCTAssertTrue(success, "auth command expected to succeed")
            XCTAssert(cmd.response! == RedisResponse(stringVal: "OK"), "expecting response from Redis to be OK")
            storedExpectation1.fulfill()
        })
        
        r.setPendingCommand(cmd)
        
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
        
        r.disconnect()
        r.connect()
        
        // test that it works again
        let storedExpectation2 = expectation(description: "set command handler activated second time")
        let cmd2 = RedisCommand.Auth(ConnectionParams.auth, handler: { success, cmd in
            XCTAssertTrue(success, "auth command expected to succeed again")
            XCTAssert(cmd.response! == RedisResponse(stringVal: "OK"), "expecting second response from Redis to be OK")
            storedExpectation2.fulfill()
        })
        
        r.setPendingCommand(cmd2)
        
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
    }
    
    func testSavingDataWithoutAuthentication()
    {
        let r = RedisConnection(serverAddress: ConnectionParams.serverAddress, serverPort: ConnectionParams.serverPort)
        r.connect()
        
        // test that it works once
        let storedExpectation1 = expectation(description: "set command handler activated")
        let cmd = RedisCommand.Set("A", valueToSet: "1", handler: { success, cmd in
            XCTAssertFalse(success, "set command expected to fail")
            XCTAssert(cmd.response! == RedisResponse(errorVal: "NOAUTH Authentication required"), "expecting response from Redis to be NOAUTH Authentication Required")
            storedExpectation1.fulfill()
        })
        
        r.setPendingCommand(cmd)
        
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
        
        // now ensure that works the second time too
        let storedExpectation2 = expectation(description: "set command handler activated again")
        let cmd2 = RedisCommand.Set("A", valueToSet: "1", handler: { success, cmd in
            XCTAssertFalse(success, "set command expected to fail")
            XCTAssert(cmd.response! == RedisResponse(errorVal: "NOAUTH Authentication required"), "expecting response from Redis to be NOAUTH Authentication Required")
            storedExpectation2.fulfill()
        })
        
        r.setPendingCommand(cmd2)
        
        waitForExpectations(timeout: 2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
    }
}

class RedisParserTests: XCTestCase {
    func testRedisResponse()
    {
        var r = RedisResponse(stringVal: "abc")
        XCTAssert(r.responseType == .string)
        
        r = RedisResponse(intVal: 3)
        XCTAssert(r.responseType == .int)
        
        r = RedisResponse(errorVal: "abc")
        XCTAssert(r.responseType == .error)
        
        r = RedisResponse(dataVal: Data())
        XCTAssert(r.responseType == .data)
        
        r = RedisResponse(dataVal: NSMutableData() as Data)
        XCTAssert(r.responseType == .data)
        
        r = RedisResponse(arrayVal: [RedisResponse(intVal: 1), RedisResponse(stringVal: "hi")])
        XCTAssert(r.responseType == .array)
        
        
        XCTAssert(RedisResponse(intVal: 1) == RedisResponse(intVal: 1))
        XCTAssert(RedisResponse(intVal: 1) != RedisResponse(intVal: 2))
        XCTAssert(RedisResponse(stringVal: "a") != RedisResponse(intVal: 2))
        XCTAssert(RedisResponse(arrayVal: [RedisResponse(stringVal: "a"), RedisResponse(errorVal: "err")]) == RedisResponse(arrayVal: [RedisResponse(stringVal: "a"), RedisResponse(errorVal: "err")]))
        XCTAssert(RedisResponse(arrayVal: [RedisResponse(stringVal: "a"), RedisResponse(errorVal: "err")]) != RedisResponse(arrayVal: [RedisResponse(stringVal: "a"), RedisResponse(errorVal: "err1")]))
    }
    
    func testRedisParser()
    {
        let parser = RedisResponseParser()
        
        let resp1 = "+OK\r\n"
        parser.storeReceivedData(resp1.data(using: String.Encoding.utf8)!)
        
        XCTAssertEqual(parser.haveResponse, true)
        XCTAssert(parser.lastResponse! == RedisResponse(stringVal: "OK"))
        
        
        parser.storeReceivedString("+")
        XCTAssertEqual(parser.haveResponse, false)
        parser.storeReceivedString("OK")
        XCTAssertEqual(parser.haveResponse, false)
        parser.storeReceivedString("\r\n")
        XCTAssertEqual(parser.haveResponse, true)
        XCTAssert(parser.lastResponse! == RedisResponse(stringVal: "OK"))
        
        parser.storeReceivedString("+")
        XCTAssertEqual(parser.haveResponse, false)
        parser.storeReceivedString("OK")
        XCTAssertEqual(parser.haveResponse, false)
        parser.storeReceivedString("\r")
        XCTAssertEqual(parser.haveResponse, false)
        parser.storeReceivedString("\r\n")
        XCTAssertEqual(parser.haveResponse, true)
        XCTAssert(parser.lastResponse! == RedisResponse(stringVal: "OK\r"))
        
        parser.storeReceivedString(":476\r\n")
        XCTAssert(parser.lastResponse! == RedisResponse(intVal: 476))
        
        parser.storeReceivedString("$12\r\nabcde\r\nfghij\r\n")
        let respData = "abcde\r\nfghij".data(using: String.Encoding.utf8)
        XCTAssert(parser.lastResponse! == RedisResponse(dataVal: respData))
        
        let data2 = "hello\r\n, world".data(using: String.Encoding.utf8)
        parser.storeReceivedString("$\(data2!.count)\r\n")
        parser.storeReceivedData(data2!)
        XCTAssertEqual(parser.haveResponse, false)
        parser.storeReceivedString("\r\n")
        XCTAssertEqual(parser.haveResponse, true)
        XCTAssert(parser.lastResponse! == RedisResponse(dataVal: data2))
        
        parser.storeReceivedString(":123\r\n")
        XCTAssert(parser.lastResponse! == RedisResponse(intVal: 123))
        
        
        parser.storeReceivedString("$12\r\nabcde\r\nfghij")
        XCTAssertEqual(parser.haveResponse, false)
        parser.storeReceivedString("\r\n")
        XCTAssertEqual(parser.haveResponse, true)
        XCTAssert(parser.lastResponse! == RedisResponse(dataVal: respData))
        
    }
    
    func testArrayParsing()
    {
        let parser = RedisResponseParser()
        
        parser.storeReceivedString("*3\r\n+subscribe\r\n+ev1\r\n:1\r\n")
        XCTAssertEqual(parser.haveResponse, true)
        if parser.haveResponse {
            XCTAssert(parser.lastResponse! == RedisResponse(arrayVal: [
                RedisResponse(stringVal: "subscribe"),
                RedisResponse(stringVal: "ev1"),
                RedisResponse(intVal: 1)
                ]))
        }
        
    }
    
    
    func testAbortWhileParsing()
    {
        // this class allows us to test whether the parser correctly reports the abort to its delegate
        class ParserDelegateForTestingAbort : RedisResponseParserDelegate {
            var errorReported = false
            var responseReported = false
            var abortReported = false
            
            func errorParsingResponse(_ error: String?) {
                errorReported = true
            }
            func parseOperationAborted() {
                abortReported = true
            }
            func receivedResponse(_ response: RedisResponse) {
                responseReported = true
            }
            func reset() {
                errorReported = false
                abortReported = false
                responseReported = false
            }
        }
        let d = ParserDelegateForTestingAbort()
        
        let parser = RedisResponseParser()
        parser.setDelegate(d)

        
        // in the middle of processing an array element
        parser.storeReceivedString("*4\r\n+sub")
        XCTAssertEqual(parser.haveResponse, false)
        parser.abortParsing()
        
        XCTAssertFalse(d.errorReported, "Parser should not report an error to delegate")
        XCTAssertTrue(d.abortReported, "Parser should report an abort to delegate")
        XCTAssertFalse(d.responseReported, "Parser should not report a response to delegate")
        
        d.reset()
        
        /// ensure can now process a full array
        parser.storeReceivedString("*3\r\n+subscribe\r\n+ev1\r\n:1\r\n")
        XCTAssertEqual(parser.haveResponse, true)
        if parser.haveResponse {
            XCTAssert(parser.lastResponse! == RedisResponse(arrayVal: [
                RedisResponse(stringVal: "subscribe"),
                RedisResponse(stringVal: "ev1"),
                RedisResponse(intVal: 1)
                ]))
        }
        
        XCTAssertFalse(d.errorReported, "Parser should not report an error to delegate")
        XCTAssertFalse(d.abortReported, "Parser should not report an abort to delegate")
        XCTAssertTrue(d.responseReported, "Parser should report a response to delegate")

    
        // in the middle of processing an array
        d.reset()
        parser.storeReceivedString("*4\r\n+sub\r\n")
        XCTAssertEqual(parser.haveResponse, false)
        parser.abortParsing()
        
        XCTAssertFalse(d.errorReported, "Parser should not report an error to delegate")
        XCTAssertTrue(d.abortReported, "Parser should report an abort to delegate")
        XCTAssertFalse(d.responseReported, "Parser should not report a response to delegate")
        
        
        // in the middle of processing a string
        d.reset()
        parser.storeReceivedString("+sub")
        XCTAssertEqual(parser.haveResponse, false)
        parser.abortParsing()
        
        XCTAssertFalse(d.errorReported, "Parser should not report an error to delegate")
        XCTAssertTrue(d.abortReported, "Parser should report an abort to delegate")
        XCTAssertFalse(d.responseReported, "Parser should not report a response to delegate")
    
    }
    
    func testErrorHandling()
    {
        
        // TODO:  test error handling
        
    }
    
    
    func testRedisBuffer()
    {
        let buf = RedisBuffer()
        
        // simple test
        buf.storeReceivedString("123")
        XCTAssertEqual(buf.getNextStringOfSize(1), "1")
        XCTAssertEqual(buf.getNextStringOfSize(1), "2")
        XCTAssertEqual(buf.getNextStringOfSize(1), "3")
        XCTAssertEqual(buf.getNextStringOfSize(1), nil)
        
        // now get longer string back
        buf.storeReceivedString("123")
        XCTAssertEqual(buf.getNextStringOfSize(3), "123")
        XCTAssertEqual(buf.getNextStringOfSize(1), nil)
        
        // store in parts
        buf.storeReceivedString("1")
        buf.storeReceivedString("2")
        buf.storeReceivedString("34")
        XCTAssertEqual(buf.getNextStringOfSize(4), "1234")
        XCTAssertEqual(buf.getNextStringOfSize(1), nil)
        
        // ensure nil if not enough data
        buf.storeReceivedString("123")
        XCTAssertEqual(buf.getNextStringOfSize(4), nil)
        XCTAssertEqual(buf.getNextStringOfSize(3), "123")
        XCTAssertEqual(buf.getNextStringOfSize(1), nil)
        
        
        // basic "CRLF" behavior
        buf.storeReceivedString("abcde\r\n")
        XCTAssertEqual(buf.getNextStringUntilCRLF(), "abcde")
        XCTAssertEqual(buf.getNextStringUntilCRLF(), nil)
        XCTAssertEqual(buf.getNextStringOfSize(1), nil)
        
        // partial string behavior
        buf.storeReceivedString("abcde\r")
        XCTAssertEqual(buf.getNextStringUntilCRLF(), nil)
        XCTAssertEqual(buf.getNextStringOfSize(1), "a")
        buf.storeReceivedString("\n")
        XCTAssertEqual(buf.getNextStringUntilCRLF(), "bcde")
        XCTAssertEqual(buf.getNextStringOfSize(1), nil)
        XCTAssertEqual(buf.getNextStringUntilCRLF(), nil)
        
    }
    
    func testRedisCommands()
    {
        let getCmd = RedisCommand.Get("aKey", handler: nil)
        XCTAssertEqual(getCmd.getCommandString(), "*2\r\n$3\r\nGET\r\n$4\r\naKey\r\n".data(using: String.Encoding.utf8))
        
        let authCmd = RedisCommand.Auth("12345", handler: nil)
        XCTAssertEqual(authCmd.getCommandString(), "*2\r\n$4\r\nAUTH\r\n$5\r\n12345\r\n".data(using: String.Encoding.utf8))
        
        
        let setCmd = RedisCommand.Set("aKey", valueToSet: "abc".data(using: String.Encoding.utf8)!, handler: nil)
        XCTAssertEqual(setCmd.getCommandString(), "*3\r\n$3\r\nSET\r\n$4\r\naKey\r\n$3\r\nabc\r\n".data(using: String.Encoding.utf8))
        
        
        let genericCmd = RedisCommand.Generic("SET", "mykey", "1", "EX", "3", handler: nil)

        XCTAssertEqual(String(data: genericCmd.getCommandString()!, encoding: String.Encoding.utf8), "*5\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$1\r\n1\r\n$2\r\nEX\r\n$1\r\n3\r\n")
        
        
        
    }
    
}
