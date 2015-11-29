//
//  RedisResponse.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation

class RedisResponse : CustomStringConvertible {
    
    enum ResponseType { case Int, String, Data, Error, Array, Unknown }
    
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

    private var stringValInternal: String?
    
    var stringVal: String? {
        get {
            switch responseType {
            case .String:
                return self.stringValInternal
            case .Data:
                return String(NSString(data: self.dataVal!, encoding: NSUTF8StringEncoding))
            case .Int:
                return String(intVal)
            case .Error:
                return nil
            case .Unknown:
                return nil
            case .Array:
                var ar: [String?] = []
                for elem in arrayVal! {
                    ar += [elem.stringVal]
                }
                return String(ar)
            }
        }
        set(value) {
            stringValInternal = value
        }
    }
    
    var dataVal: NSData?
    var errorVal: String?
    var arrayVal: [RedisResponse]?
    
    var parseErrorMsg: String? = nil
    
    init(intVal: Int? = nil, dataVal: NSData? = nil, stringVal: String? = nil, errorVal: String? = nil, arrayVal: [RedisResponse]? = nil, responseType: ResponseType? = nil)
    {
        self.parseErrorMsg = nil
        self.intVal = intVal
        self.stringValInternal = stringVal
        self.errorVal = errorVal
        self.arrayVal = arrayVal
        self.dataVal = dataVal
        
        if intVal != nil { self.responseType = .Int }
        else if stringVal != nil { self.responseType = .String }
        else if errorVal != nil { self.responseType = .Error }
        else if arrayVal != nil { self.responseType = .Array }
        else if dataVal != nil { self.responseType = .Data }
        else if responseType != nil { self.responseType = responseType! }
        else { self.responseType = .Unknown }
        
        if self.responseType == .Array && self.arrayVal == nil {
            self.arrayVal = [RedisResponse]()
        }
    }
    
    func parseError(msg: String)
    {
        NSLog("Parse error - \(msg)")
        parseErrorMsg = msg
    }
    
    // should only be called if the current object responseType is .Array
    func addArrayElement(response: RedisResponse)
    {
        self.arrayVal!.append(response)
    }
    
    var description: String {
        var result = "RedisResponse(\(self.responseType):"
        
        switch self.responseType {
        case .Error:
            result += self.errorVal!
        case .String:
            result += self.stringVal!
        case .Int:
            result += String(self.intVal!)
        case .Data:
            result += "[Data - \(self.dataVal!.length) bytes]"
        case .Array:
            result += "[Array - \(self.arrayVal!.count) elements]"
        case .Unknown:
            result += "?"
            break
        }
        
        result += ")"
        
        return result
    }
    
    // reads data from the buffer according to own responseType
    func readValueFromBuffer(buffer: RedisBuffer, numBytes: Int?) -> Bool?
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
            if crlfData!.length != 0 {
                buffer.restoreRemovedData(crlfData!)
                return false
            }
        }
        
        switch responseType {
        case .Data:
            self.dataVal = data
            return true
            
        case .String:
            self.stringVal = String(data: data!, encoding: NSUTF8StringEncoding)
            if ( self.stringVal == nil ) {
                parseError("Could not parse string")
                return false
            }
            return true
        case .Error:
            self.errorVal = String(data: data!, encoding: NSUTF8StringEncoding)
            if ( self.errorVal == nil ) {
                parseError("Could not parse string")
                return false
            }
            return true
        case .Int:
            let str = String(data: data!, encoding: NSUTF8StringEncoding)
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
            
        case .Array:
            parseError("Array parsing not supported yet")
            return false
            
        case .Unknown:
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
    case .Int:
        return left.intVal == right.intVal
    case .String:
        return left.stringVal == right.stringVal
    case .Error:
        return left.errorVal == right.errorVal
    case .Unknown:
        return true
    case .Data:
        return left.dataVal!.isEqualToData(right.dataVal!)
    case .Array:
        if left.arrayVal!.count != right.arrayVal!.count { return false }
        for i in 0 ... left.arrayVal!.count-1 {
            if !(left.arrayVal![i] == right.arrayVal![i]) { return false }
        }
        return true
    }
}