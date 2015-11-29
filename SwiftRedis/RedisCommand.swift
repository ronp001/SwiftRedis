//
//  RedisCommand.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation

protocol RedisCommandDelegate {
    func commandExecuted(cmd: RedisCommand)
    func commandFailed(cmd: RedisCommand)
}

class RedisCommand : CustomStringConvertible
{
    var delegate: RedisCommandDelegate? = nil
    
    var description: String {
        return "RedisCommand(\(commandType))"
    }
    
    enum Type { case Get, Set, Auth, Publish, Subscribe, Quit, Generic }
    typealias ValueCompletionHandler = (success: Bool, key: String, result: RedisResponse?, cmd: RedisCommand) -> Void
    typealias VoidCompletionHandler = (success: Bool, cmd: RedisCommand) -> Void
    
    var sent = false // connection object sets this to true when the command is sent
    
    let commandType: Type
    var param1: String?
    var param2: NSData?
    var additionalParams: [String?]?
    
    let valueCompletionHandler: ValueCompletionHandler?
    let voidCompletionHandler: VoidCompletionHandler?
    let finishWhenResponseReceived: Bool
    
    var key: String? { get { return param1 }  set(value) { param1 = value } }
    var valueToSet: NSData? { get { return param2 } set(value) {param2 = value} }
    
    init(type: Type, param1: String? = nil, param2: NSData? = nil, valueCompletionHandler: ValueCompletionHandler? = nil, voidCompletionHandler: VoidCompletionHandler? = nil, additionalParams: [String?]? = nil )
    {
        self.commandType = type
        self.param1 = param1
        self.param2 = param2
        self.valueCompletionHandler = valueCompletionHandler
        self.voidCompletionHandler = voidCompletionHandler
        self.additionalParams = additionalParams
        
        finishWhenResponseReceived = (type != .Subscribe)
    }
    
    // call the completion handler with a failure status
    func completionFailed()
    {
        switch self.commandType {
        case .Get, .Subscribe, .Publish, .Generic:
            valueCompletionHandler?(success: false, key: key!, result: nil, cmd: self)
        case .Set, .Auth, .Quit:
            voidCompletionHandler?(success: false, cmd: self)
        }

        delegate?.commandFailed(self)
    }
    
    var response: RedisResponse? = nil
    
    func responseReceived(response: RedisResponse)
    {
        self.response = response

        print("received response: \(self.response)")
        
        var success = true
        if response.responseType == .Error {
            success = false
        }
        
        switch self.commandType {
        case .Get, .Publish, .Subscribe, .Generic:
            valueCompletionHandler?(success: success, key: key!, result: response, cmd: self)
        case .Set, .Auth, .Quit:
            voidCompletionHandler?(success: success, cmd: self)
        }

        delegate?.commandExecuted(self)
    }
    
    static func Quit(handler: VoidCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .Quit, voidCompletionHandler: handler)
    }
    
    static func Auth(password: String, handler: VoidCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .Auth, param1: password, voidCompletionHandler: handler)
    }
    
    static func Set(key: String, valueToSet: NSData, handler: VoidCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .Set, param1: key, param2: valueToSet, voidCompletionHandler: handler)
    }
    
    static func Set(key: String, valueToSet: String, handler: VoidCompletionHandler?) -> RedisCommand
    {
        return Set(key, valueToSet: valueToSet.dataUsingEncoding(NSUTF8StringEncoding)!, handler: handler)
    }
    
    static func Get(key: String, handler: ValueCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .Get, param1: key, valueCompletionHandler: handler)
    }
    
    static func Publish(channel: String, value: String, handler: ValueCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .Publish, param1: channel, param2: value.dataUsingEncoding(NSUTF8StringEncoding)!, valueCompletionHandler: handler)
    }
    
    static func Subscribe(channel: String, handler: ValueCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .Subscribe, param1: channel, valueCompletionHandler: handler)
    }
    
    static func Generic(cmd: String, _ arg1: String? = nil, _ arg2: String? = nil, _ arg3: String? = nil, _ arg4: String? = nil, handler: ValueCompletionHandler? ) -> RedisCommand
    {
        return RedisCommand(type: .Generic, param1: cmd, param2: nil, valueCompletionHandler: handler, voidCompletionHandler: nil, additionalParams: [arg1, arg2, arg3, arg4])
    }
    
    func buildCommandString(words: [NSObject]) -> NSData
    {
        let result = NSMutableData(data: "*\(words.count)\r\n".dataUsingEncoding(NSUTF8StringEncoding)!)
        
        for word in words {
            if let wordStr = word as? NSString {
                let lenStr = NSData(data: "$\(wordStr.length)\r\n".dataUsingEncoding(NSUTF8StringEncoding)!)
                result.appendData(lenStr)
                
                let strStr = NSData(data: "\(wordStr)\r\n".dataUsingEncoding(NSUTF8StringEncoding)!)
                result.appendData(strStr)
            } else if let wordData = word as? NSData {
                let lenStr = NSData(data: "$\(wordData.length)\r\n".dataUsingEncoding(NSUTF8StringEncoding)!)
                result.appendData(lenStr)
                
                result.appendData(wordData)
                result.appendData("\r\n".dataUsingEncoding(NSUTF8StringEncoding)!)
            } else {
                assert(false)
            }
        }
        
        return result
    }
    
    func getCommandString() -> NSData? {
        
        switch commandType {
        case .Get:
            return buildCommandString(["GET", self.param1!])
        case .Set:
            return buildCommandString(["SET", self.param1!, self.param2!])
        case .Publish:
            return buildCommandString(["PUBLISH", self.param1!, self.param2!])
        case .Subscribe:
            return buildCommandString(["SUBSCRIBE", self.param1!])
        case .Auth:
            return buildCommandString(["AUTH", self.param1!])
        case .Quit:
            return buildCommandString(["QUIT"])
        case .Generic:
            var cmdArray: [String] = []
            cmdArray += [self.param1!]
            if let additionalParams = self.additionalParams {
                for param in additionalParams {
                    if let param = param {
                        cmdArray += [param]
                    }
                }
            }
            return buildCommandString(cmdArray)
        }
    }
}
