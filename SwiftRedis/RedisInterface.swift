//
//  RedisInterface.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation

class RedisInterface : RedisCommandDelegate, RedisConnectionDelegate
{
    // MARK: properties
    let c: RedisConnection
    let auth: String
    
    init(host: String, port: UInt32, auth: String)
    {
        self.c = RedisConnection(serverAddress: host, serverPort: port)
        self.auth = auth
    }
    
    var commandQueue = [RedisCommand]()
    var currentCommand: RedisCommand? = nil
    var isConnected = false

    func connect()
    {
        c.delegate = self
        c.connect()
    }
    
    func addCommandToQueue(command: RedisCommand)
    {
        commandQueue.append(command)
        sendNextCommandIfPossible()
    }
    
    func sendNextCommandIfPossible()
    {
        if !isConnected { return }
        if commandQueue.count == 0 { return }
        
        if currentCommand != nil { return }
        
        currentCommand = commandQueue.removeAtIndex(0)
        currentCommand?.delegate = self
        
        print("sending next command: \(currentCommand)")
        c.setPendingCommand(currentCommand!)
    }

    func authenticationFailed()
    {
        print("Redis authentication failed")
        abort()
    }
    
    // MARK: RedisConnectionDelegate functions
    
    func connectionError(error: String) {
        print("RedisInterface: reconnecting due to connection error \(error)")
        c.disconnect()
        c.connect()
    }

    func connected()
    {
        isConnected = true
        commandQueue.insert(RedisCommand.Auth(self.auth, handler: {success, cmd in
            if !success {
                self.authenticationFailed()
            }
        }), atIndex: 0)
        
        sendNextCommandIfPossible()
    }
    
    // MARK: RedisCommandDelegate functions

    func commandExecuted(cmd: RedisCommand) {
        self.currentCommand = nil
        sendNextCommandIfPossible()
    }
    
    func commandFailed(cmd: RedisCommand) {
        self.currentCommand = nil
        sendNextCommandIfPossible()
    }

    // MARK: operational interface
    
    func setDataForKey(key: String, data: NSData, completionHandler: RedisCommand.VoidCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Set(key, valueToSet: data, handler: completionHandler))
    }
    
    func getDataForKey(key: String, completionHandler: RedisCommand.ValueCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Get(key, handler: completionHandler))
    }
    
    func subscribe(channel: String, completionHandler: RedisCommand.ValueCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Subscribe(channel, handler: completionHandler))
    }

    func publish(channel: String, value: String, completionHandler: RedisCommand.ValueCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Publish(channel, value: value, handler: completionHandler))
    }
    
}

