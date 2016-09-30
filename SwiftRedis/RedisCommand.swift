//
//  RedisCommand.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation

protocol RedisCommandDelegate {
    func commandExecuted(_ cmd: RedisCommand)
    func commandFailed(_ cmd: RedisCommand)
}

class RedisCommand : CustomStringConvertible
{
    var delegate: RedisCommandDelegate? = nil
    
    var description: String {
        return "RedisCommand(\(commandType))"
    }
    
    enum Type { case get, set, auth, publish, subscribe, quit, generic }
    typealias ValueCompletionHandler = (_ success: Bool, _ key: String, _ result: RedisResponse?, _ cmd: RedisCommand) -> Void
    typealias VoidCompletionHandler = (_ success: Bool, _ cmd: RedisCommand) -> Void
    
    var sent = false // connection object sets this to true when the command is sent
    
    let commandType: Type
    var param1: String?
    var param2: Data?
    var additionalParams: [String?]?
    
    let valueCompletionHandler: ValueCompletionHandler?
    let voidCompletionHandler: VoidCompletionHandler?
    let finishWhenResponseReceived: Bool
    
    var key: String? { get { return param1 }  set(value) { param1 = value } }
    var valueToSet: Data? { get { return param2 } set(value) {param2 = value} }
    
    init(type: Type, param1: String? = nil, param2: Data? = nil, valueCompletionHandler: ValueCompletionHandler? = nil, voidCompletionHandler: VoidCompletionHandler? = nil, additionalParams: [String?]? = nil )
    {
        self.commandType = type
        self.param1 = param1
        self.param2 = param2
        self.valueCompletionHandler = valueCompletionHandler
        self.voidCompletionHandler = voidCompletionHandler
        self.additionalParams = additionalParams
        
        finishWhenResponseReceived = (type != .subscribe)
    }
    
    // call the completion handler with a failure status
    func completionFailed()
    {
        switch self.commandType {
        case .get, .subscribe, .publish, .generic:
            valueCompletionHandler?(false, key!, nil, self)
        case .set, .auth, .quit:
            voidCompletionHandler?(false, self)
        }

        delegate?.commandFailed(self)
    }
    
    var response: RedisResponse? = nil
    
    func responseReceived(_ response: RedisResponse)
    {
        self.response = response

        print("received response: \(self.response)")
        
        var success = true
        if response.responseType == .error {
            success = false
        }
        
        switch self.commandType {
        case .get, .publish, .subscribe, .generic:
            valueCompletionHandler?(success, key!, response, self)
        case .set, .auth, .quit:
            voidCompletionHandler?(success, self)
        }

        delegate?.commandExecuted(self)
    }
    
    static func Quit(_ handler: VoidCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .quit, voidCompletionHandler: handler)
    }
    
    static func Auth(_ password: String, handler: VoidCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .auth, param1: password, voidCompletionHandler: handler)
    }
    
    static func Set(_ key: String, valueToSet: Data, handler: VoidCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .set, param1: key, param2: valueToSet, voidCompletionHandler: handler)
    }
    
    static func Set(_ key: String, valueToSet: String, handler: VoidCompletionHandler?) -> RedisCommand
    {
        return Set(key, valueToSet: valueToSet.data(using: String.Encoding.utf8)!, handler: handler)
    }
    
    static func Get(_ key: String, handler: ValueCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .get, param1: key, valueCompletionHandler: handler)
    }
    
    static func Publish(_ channel: String, value: String, handler: ValueCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .publish, param1: channel, param2: value.data(using: String.Encoding.utf8)!, valueCompletionHandler: handler)
    }
    
    static func Subscribe(_ channel: String, handler: ValueCompletionHandler?) -> RedisCommand
    {
        return RedisCommand(type: .subscribe, param1: channel, valueCompletionHandler: handler)
    }
    
    static func Generic(_ cmd: String, _ arg1: String? = nil, _ arg2: String? = nil, _ arg3: String? = nil, _ arg4: String? = nil, handler: ValueCompletionHandler? ) -> RedisCommand
    {
        return RedisCommand(type: .generic, param1: cmd, param2: nil, valueCompletionHandler: handler, voidCompletionHandler: nil, additionalParams: [arg1, arg2, arg3, arg4])
    }
    
    func buildCommandString(_ words: [NSObject]) -> Data
    {
        var result = NSData(data: "*\(words.count)\r\n".data(using: String.Encoding.utf8)!) as Data
        
        for word in words {
            if let wordStr = word as? NSString {
                let lenStr = NSData(data: "$\(wordStr.length)\r\n".data(using: String.Encoding.utf8)!) as Data
                result.append(lenStr)
                
                let strStr = NSData(data: "\(wordStr)\r\n".data(using: String.Encoding.utf8)!) as Data
                result.append(strStr)
            } else if let wordData = word as? Data {
                let lenStr = NSData(data: "$\(wordData.count)\r\n".data(using: String.Encoding.utf8)!) as Data
                result.append(lenStr)
                
                result.append(wordData)
                result.append("\r\n".data(using: String.Encoding.utf8)!)
            } else {
                assert(false)
            }
        }
        
        return result
    }
    
    func getCommandString() -> Data? {
        
        switch commandType {
        case .get:
            return buildCommandString(["GET" as NSObject, self.param1! as NSObject])
        case .set:
            return buildCommandString(["SET" as NSObject, self.param1! as NSObject, self.param2! as NSObject])
        case .publish:
            return buildCommandString(["PUBLISH" as NSObject, self.param1! as NSObject, self.param2! as NSObject])
        case .subscribe:
            return buildCommandString(["SUBSCRIBE" as NSObject, self.param1! as NSObject])
        case .auth:
            return buildCommandString(["AUTH" as NSObject, self.param1! as NSObject])
        case .quit:
            return buildCommandString(["QUIT" as NSObject])
        case .generic:
            var cmdArray: [String] = []
            cmdArray += [self.param1!]
            if let additionalParams = self.additionalParams {
                for param in additionalParams {
                    if let param = param {
                        cmdArray += [param]
                    }
                }
            }
            return buildCommandString(cmdArray as [NSObject])
        }
    }
}
