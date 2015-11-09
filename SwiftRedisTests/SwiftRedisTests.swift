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
    func testSetAndGet()
    {
        let r = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
        
        r.connect()
        
        let data1 = "hi".dataUsingEncoding(NSUTF8StringEncoding)!
        let data2 = "hello, world".dataUsingEncoding(NSUTF8StringEncoding)!
        
        
        // store data1
        let storedExpectation1 = expectationWithDescription("testkey1 stored")
        r.setDataForKey("testkey1", data: data1, completionHandler: { success, cmd in
            XCTAssertTrue(success, "expecting success storing data for testkey1")
            storedExpectation1.fulfill()
        })
        waitForExpectationsWithTimeout(2, handler: { error in
            XCTAssertNil(error, "expecting operation to succeed")
        })
        
        // retrieve data1
        let storedExpectation2 = expectationWithDescription("testkey1 retrieved")
        r.getDataForKey("testkey1", completionHandler: { success, key, data, cmd in
            storedExpectation2.fulfill()
            XCTAssertTrue(success)
            XCTAssert(key == "testkey1")
            XCTAssert(data! == RedisResponse(dataVal: data1))
        })
        waitForExpectationsWithTimeout(2, handler: { error in
            XCTAssertNil(error, "expecting operation to succeed")
        })
        
        
        // store data2
        let storedExpectation3 = expectationWithDescription("testkey2 stored")
        r.setDataForKey("testkey2", data: data2, completionHandler: { success, cmd in
            XCTAssertTrue(success, "could not store testkey2")
            storedExpectation3.fulfill()
        })
        waitForExpectationsWithTimeout(5, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
        // retrieve data2
        let storedExpectation4 = expectationWithDescription("testkey2 stored")
        r.getDataForKey("testkey2", completionHandler: { success, key, data, cmd in
            storedExpectation4.fulfill()
            XCTAssertTrue(success)
            XCTAssert(key == "testkey2")
            XCTAssert(data! == RedisResponse(dataVal: data2))
        })
        waitForExpectationsWithTimeout(5, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
    }
    
    
    func testPubSub()
    {
        let riPublish = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
        riPublish.connect()
        
        let riSubscribe = RedisInterface(host: ConnectionParams.serverAddress, port: ConnectionParams.serverPort, auth: ConnectionParams.auth)
        riSubscribe.connect()
        
        let expectingSubscribeToReturn1 = expectationWithDescription("subscribe operation returned once")
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
                    RedisResponse(dataVal: "subscribe".dataUsingEncoding(NSUTF8StringEncoding)),
                    RedisResponse(dataVal: "testchannel".dataUsingEncoding(NSUTF8StringEncoding)),
                    RedisResponse(intVal: 1)
                    ]))
                XCTAssertNotNil(expectingSubscribeToReturn1)
                expectingSubscribeToReturn1.fulfill()
                
                subscribeReturnCount++
                
            case 1:
                XCTAssertTrue(success)
                XCTAssert(channel == "testchannel")
                XCTAssert(data! == RedisResponse(arrayVal: [
                    RedisResponse(dataVal: "message".dataUsingEncoding(NSUTF8StringEncoding)),
                    RedisResponse(dataVal: "testchannel".dataUsingEncoding(NSUTF8StringEncoding)),
                    RedisResponse(dataVal: "publish op 1".dataUsingEncoding(NSUTF8StringEncoding)),
                    ]))
                
                XCTAssertNotNil(expectingSubscribeToReturn2)
                expectingSubscribeToReturn2!.fulfill()
                subscribeReturnCount++
                
            case 2:
                XCTAssertTrue(success)
                XCTAssert(channel == "testchannel")
                XCTAssert(data! == RedisResponse(arrayVal: [
                    RedisResponse(dataVal: "message".dataUsingEncoding(NSUTF8StringEncoding)),
                    RedisResponse(dataVal: "testchannel".dataUsingEncoding(NSUTF8StringEncoding)),
                    RedisResponse(dataVal: "publish op 2".dataUsingEncoding(NSUTF8StringEncoding)),
                    ]))
                
                XCTAssertNotNil(expectingSubscribeToReturn3)
                expectingSubscribeToReturn3!.fulfill()
                subscribeReturnCount++
                
            default:
                XCTAssert(false)
            }
        })
        
        // wait for the subscribe operation to complete
        waitForExpectationsWithTimeout(1, handler: { error in
            XCTAssertNil(error, "expecting operation to succeed")
        })
        
        XCTAssertEqual(subscribeReturnCount, 1)
        // -----
        
        // publish something to the test channel
        expectingSubscribeToReturn2 = expectationWithDescription("subscribe operation returned twice")
        let expectingPublishToComplete1 = expectationWithDescription("publish operation 1 completed")
        riPublish.publish("testchannel", value: "publish op 1", completionHandler: { success, key, data, cmd in
            expectingPublishToComplete1.fulfill()
        })
        
        // wait for both the publish to complete, and the subscribe to return the 2nd time
        waitForExpectationsWithTimeout(1, handler: { error in
            XCTAssertNil(error, "expecting publish 1 to complete, and subscribe to return 2nd time")
        })
        
        XCTAssertEqual(subscribeReturnCount, 2)
        
        // -----
        
        // publish something else to the test channel
        expectingSubscribeToReturn3 = expectationWithDescription("subscribe operation returned third time")
        let expectingPublishToComplete2 = expectationWithDescription("publish operation 2 completed")
        riPublish.publish("testchannel", value: "publish op 2", completionHandler: { success, key, data, cmd in
            expectingPublishToComplete2.fulfill()
        })
        
        // wait for both the publish to complete, and the subscribe to return the 2nd time
        waitForExpectationsWithTimeout(2, handler: { error in
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
        let storedExpectation1 = expectationWithDescription("set command handler activated")
        let cmd = RedisCommand.Auth(ConnectionParams.auth, handler: { success, cmd in
            XCTAssertTrue(success, "auth command expected to succeed")
            XCTAssert(cmd.response! == RedisResponse(stringVal: "OK"), "expecting response from Redis to be OK")
            storedExpectation1.fulfill()
        })
        
        r.setPendingCommand(cmd)
        
        waitForExpectationsWithTimeout(2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
    }
    
    func testSavingDataWithoutAuthentication()
    {
        let r = RedisConnection(serverAddress: ConnectionParams.serverAddress, serverPort: ConnectionParams.serverPort)
        r.connect()
        
        // test that it works once
        let storedExpectation1 = expectationWithDescription("set command handler activated")
        let cmd = RedisCommand.Set("A", valueToSet: "1", handler: { success, cmd in
            XCTAssertFalse(success, "set command expected to fail")
            XCTAssert(cmd.response! == RedisResponse(errorVal: "NOAUTH Authentication required"), "expecting response from Redis to be NOAUTH Authentication Required")
            storedExpectation1.fulfill()
        })
        
        r.setPendingCommand(cmd)
        
        waitForExpectationsWithTimeout(2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
        
        // now ensure that works the second time too
        let storedExpectation2 = expectationWithDescription("set command handler activated again")
        let cmd2 = RedisCommand.Set("A", valueToSet: "1", handler: { success, cmd in
            XCTAssertFalse(success, "set command expected to fail")
            XCTAssert(cmd.response! == RedisResponse(errorVal: "NOAUTH Authentication required"), "expecting response from Redis to be NOAUTH Authentication Required")
            storedExpectation2.fulfill()
        })
        
        r.setPendingCommand(cmd2)
        
        waitForExpectationsWithTimeout(2, handler: { error in
            XCTAssertNil(error, "Error")
        })
        
    }
}

class RedisParserTests: XCTestCase {
    func testRedisResponse()
    {
        var r = RedisResponse(stringVal: "abc")
        XCTAssert(r.responseType == .String)
        
        r = RedisResponse(intVal: 3)
        XCTAssert(r.responseType == .Int)
        
        r = RedisResponse(errorVal: "abc")
        XCTAssert(r.responseType == .Error)
        
        r = RedisResponse(dataVal: NSData())
        XCTAssert(r.responseType == .Data)
        
        r = RedisResponse(dataVal: NSMutableData())
        XCTAssert(r.responseType == .Data)
        
        r = RedisResponse(arrayVal: [RedisResponse(intVal: 1), RedisResponse(stringVal: "hi")])
        XCTAssert(r.responseType == .Array)
        
        
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
        parser.storeReceivedData(resp1.dataUsingEncoding(NSUTF8StringEncoding)!)
        
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
        let respData = "abcde\r\nfghij".dataUsingEncoding(NSUTF8StringEncoding)
        XCTAssert(parser.lastResponse! == RedisResponse(dataVal: respData))
        
        let data2 = "hello\r\n, world".dataUsingEncoding(NSUTF8StringEncoding)
        parser.storeReceivedString("$\(data2!.length)\r\n")
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
        XCTAssertEqual(getCmd.getCommandString(), "*2\r\n$3\r\nGET\r\n$4\r\naKey\r\n".dataUsingEncoding(NSUTF8StringEncoding))
        
        let authCmd = RedisCommand.Auth("12345", handler: nil)
        XCTAssertEqual(authCmd.getCommandString(), "*2\r\n$4\r\nAUTH\r\n$5\r\n12345\r\n".dataUsingEncoding(NSUTF8StringEncoding))
        
        
        let setCmd = RedisCommand.Set("aKey", valueToSet: "abc".dataUsingEncoding(NSUTF8StringEncoding)!, handler: nil)
        XCTAssertEqual(setCmd.getCommandString(), "*3\r\n$3\r\nSET\r\n$4\r\naKey\r\n$3\r\nabc\r\n".dataUsingEncoding(NSUTF8StringEncoding))
    }
    
}
