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
    
    deinit
    {
        disconnect()
    }
    
    var commandQueue = [RedisCommand]()
    var currentCommand: RedisCommand? = nil
    var isConnected = false

    func connect()
    {
        c.delegate = self
        c.connect()
    }
    
    func disconnect()
    {
        commandQueue.removeAll()
        c.disconnect()
    }
    
    func addCommandToQueue(_ command: RedisCommand)
    {
        commandQueue.append(command)
        sendNextCommandIfPossible()
    }
    
    func skipPendingCommandsAndQuit(_ completionHandler: RedisCommand.VoidCompletionHandler? )
    {
        if commandQueue.count > 0 {
            print("RedisInterface -- skipPendingCommandsAndQuit: removing \(commandQueue.count) pending commands from redis queue")
            commandQueue.removeAll()
        } else {
            print("RedisInterface -- skipPendingCommandsAndQuit: no pending commands to remove")
        }
        self.quit(completionHandler)
    }
    
    func sendNextCommandIfPossible()
    {
        if !isConnected { return }
        if commandQueue.count == 0 { return }
        
        if currentCommand != nil { return }
        
        currentCommand = commandQueue.remove(at: 0)
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
    
    func connectionError(_ error: String) {
        print("RedisInterface: connection error \(error)")
        isConnected = false
    }

    func connected()
    {
        isConnected = true
        commandQueue.insert(RedisCommand.Auth(self.auth, handler: {success, cmd in
            if !success {
                self.authenticationFailed()
            }
        }), at: 0)
        
        sendNextCommandIfPossible()
    }
    
    // MARK: RedisCommandDelegate functions

    func commandExecuted(_ cmd: RedisCommand) {
        self.currentCommand = nil
        sendNextCommandIfPossible()
    }
    
    func commandFailed(_ cmd: RedisCommand) {
        self.currentCommand = nil
        sendNextCommandIfPossible()
    }

    // MARK: operational interface
    
    func setDataForKey(_ key: String, data: Data, completionHandler: RedisCommand.VoidCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Set(key, valueToSet: data, handler: completionHandler))
    }
    
    func setValueForKey(_ key: String, stringValue: String, completionHandler: RedisCommand.VoidCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Set(key, valueToSet: stringValue, handler: completionHandler))
    }
    
    func getDataForKey(_ key: String, completionHandler: RedisCommand.ValueCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Get(key, handler: completionHandler))
    }
    
    func getValueForKey(_ key: String, completionHandler: RedisCommand.ValueCompletionHandler? )
    {
        getDataForKey(key, completionHandler: completionHandler)
    }
    
    func subscribe(_ channel: String, completionHandler: RedisCommand.ValueCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Subscribe(channel, handler: completionHandler))
    }

    func publish(_ channel: String, value: String, completionHandler: RedisCommand.ValueCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Publish(channel, value: value, handler: completionHandler))
    }
    
    func generic(_ cmd: String, _ arg1: String? = nil, _ arg2: String? = nil, _ arg3: String? = nil, _ arg4: String? = nil, completionHandler: RedisCommand.ValueCompletionHandler?)
    {
        addCommandToQueue(RedisCommand.Generic(cmd, arg1, arg2, arg3, arg4, handler: completionHandler))
    }
    
    func quit(_ completionHandler: RedisCommand.VoidCompletionHandler? )
    {
        addCommandToQueue(RedisCommand.Quit(completionHandler))
    }
    
}

