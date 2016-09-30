//
//  RedisResponse.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation

class RedisResponse : CustomStringConvertible {
    
    enum ResponseType { case int, string, data, error, array, unknown }
    
    class ParseError : NSError {
        init(msg: String) {
            super.init(domain: "RedisResponse", code: 0, userInfo: ["message": msg])
        }
        
        required init?(coder aDecoder: NSCoder) {
            fatalError("init(coder:) has not been implemented")
        }
    }
    
    let responseType: ResponseType
    
    var intVal: Int?

    fileprivate var stringValInternal: String?
    
    var stringVal: String? {
        get {
            switch responseType {
            case .string:
                return self.stringValInternal
            case .data:
                return String(describing: NSString(data: self.dataVal!, encoding: String.Encoding.utf8.rawValue))
            case .int:
                return String(describing: intVal)
            case .error:
                return nil
            case .unknown:
                return nil
            case .array:
                var ar: [String?] = []
                for elem in arrayVal! {
                    ar += [elem.stringVal]
                }
                return String(describing: ar)
            }
        }
        set(value) {
            stringValInternal = value
        }
    }
    
    var dataVal: Data?
    var errorVal: String?
    var arrayVal: [RedisResponse]?
    
    var parseErrorMsg: String? = nil
    
    init(intVal: Int? = nil, dataVal: Data? = nil, stringVal: String? = nil, errorVal: String? = nil, arrayVal: [RedisResponse]? = nil, responseType: ResponseType? = nil)
    {
        self.parseErrorMsg = nil
        self.intVal = intVal
        self.stringValInternal = stringVal
        self.errorVal = errorVal
        self.arrayVal = arrayVal
        self.dataVal = dataVal
        
        if intVal != nil { self.responseType = .int }
        else if stringVal != nil { self.responseType = .string }
        else if errorVal != nil { self.responseType = .error }
        else if arrayVal != nil { self.responseType = .array }
        else if dataVal != nil { self.responseType = .data }
        else if responseType != nil { self.responseType = responseType! }
        else { self.responseType = .unknown }
        
        if self.responseType == .array && self.arrayVal == nil {
            self.arrayVal = [RedisResponse]()
        }
    }
    
    func parseError(_ msg: String)
    {
        NSLog("Parse error - \(msg)")
        parseErrorMsg = msg
    }
    
    // should only be called if the current object responseType is .Array
    func addArrayElement(_ response: RedisResponse)
    {
        self.arrayVal!.append(response)
    }
    
    var description: String {
        var result = "RedisResponse(\(self.responseType):"
        
        switch self.responseType {
        case .error:
            result += self.errorVal!
        case .string:
            result += self.stringVal!
        case .int:
            result += String(self.intVal!)
        case .data:
            result += "[Data - \(self.dataVal!.count) bytes]"
        case .array:
            result += "[Array - \(self.arrayVal!.count) elements]"
        case .unknown:
            result += "?"
            break
        }
        
        result += ")"
        
        return result
    }
    
    // reads data from the buffer according to own responseType
    func readValueFromBuffer(_ buffer: RedisBuffer, numBytes: Int?) -> Bool?
    {
        let data = buffer.getNextDataOfSize(numBytes)
        
        if data == nil { return nil }
        
        if numBytes != nil { // ensure there is also a CRLF after the buffer
            let crlfData = buffer.getNextDataUntilCRLF()
            if crlfData == nil {  // didn't find data
                buffer.restoreRemovedData(data!)
                return nil
            }
            
            // if the data was CRLF, it would have been removed by getNxtDataUntilCRLF, so buffer should be empty
            if crlfData!.count != 0 {
                buffer.restoreRemovedData(crlfData!)
                return false
            }
        }
        
        switch responseType {
        case .data:
            self.dataVal = data
            return true
            
        case .string:
            self.stringVal = String(data: data!, encoding: String.Encoding.utf8)
            if ( self.stringVal == nil ) {
                parseError("Could not parse string")
                return false
            }
            return true
        case .error:
            self.errorVal = String(data: data!, encoding: String.Encoding.utf8)
            if ( self.errorVal == nil ) {
                parseError("Could not parse string")
                return false
            }
            return true
        case .int:
            let str = String(data: data!, encoding: String.Encoding.utf8)
            if ( str == nil ) {
                parseError("Could not parse string")
                return false
            }
            self.intVal = Int(str!)
            if ( self.intVal == nil ) {
                parseError("Received '\(str)' when expecting Integer")
                return false
            }
            return true
            
        case .array:
            parseError("Array parsing not supported yet")
            return false
            
        case .unknown:
            parseError("Trying to parse when ResponseType == .Unknown")
            return false
        }
    }
}

func != (left: RedisResponse, right: RedisResponse) -> Bool {
    return !(left == right)
}

func == (left: RedisResponse, right: RedisResponse) -> Bool {
    if left.responseType != right.responseType { return false }
    
    switch left.responseType {
    case .int:
        return left.intVal == right.intVal
    case .string:
        return left.stringVal == right.stringVal
    case .error:
        return left.errorVal == right.errorVal
    case .unknown:
        return true
    case .data:
        return (left.dataVal! == right.dataVal!)
    case .array:
        if left.arrayVal!.count != right.arrayVal!.count { return false }
        for i in 0 ... left.arrayVal!.count-1 {
            if !(left.arrayVal![i] == right.arrayVal![i]) { return false }
        }
        return true
    }
}
