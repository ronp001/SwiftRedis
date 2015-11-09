//
//  RedisBuffer.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation

class RedisBuffer
{
    var dataAccumulatedFromStream: NSData?
    
    func restoreRemovedData(dataToRestore: NSData) {
        if dataAccumulatedFromStream == nil {
            dataAccumulatedFromStream = dataToRestore
            return
        } else {
            let combinedData = NSMutableData(data: dataToRestore)
            combinedData.appendData(dataAccumulatedFromStream!)
            dataAccumulatedFromStream = combinedData
        }
    }
    
    func removeBytesFromBuffer(numBytesToRemove: Int)
    {
        let len = dataAccumulatedFromStream!.length - numBytesToRemove
        
        let range = NSMakeRange(numBytesToRemove, len)
        let data = dataAccumulatedFromStream!.subdataWithRange(range)
        
        if data.length == 0 {
            dataAccumulatedFromStream = nil
        } else {
            dataAccumulatedFromStream = data
        }
    }
    
    
    func getNextDataOfSize(size: Int?) -> NSData?
    {
        if size == nil { return getNextDataUntilCRLF() }
        
        if dataAccumulatedFromStream == nil { return nil }
        if dataAccumulatedFromStream!.length < size! {
            return nil
        }
        
        let range = NSMakeRange(0, size!)
        let data = dataAccumulatedFromStream!.subdataWithRange(range)
        
        removeBytesFromBuffer(size!)
        
        return data
    }
    
    func getNextStringOfSize(size: Int) -> String?
    {
        let data = getNextDataOfSize(size)
        if data == nil { return nil }
        
        return String(NSString(data: data!, encoding: NSUTF8StringEncoding)!)
    }
    
    func getNextStringUntilCRLF() -> String?
    {
        let data = getNextDataUntilCRLF()
        if data == nil { return nil }
        
        return String(NSString(data: data!, encoding: NSUTF8StringEncoding)!)
    }
    
    func getNextDataUntilCRLF() -> NSData?
    {
        if dataAccumulatedFromStream == nil { return nil }
        
        let len = dataAccumulatedFromStream!.length
        
        var done = false
        var bytesProcessed = 0
        
        let charCR = UInt8(0x0d)
        let charLF = UInt8(0x0a)
        
        var foundCR = false
        
        let inputStream = NSInputStream(data: dataAccumulatedFromStream!)
        inputStream.open()
        
        while !done && bytesProcessed < len
        {
            var char: UInt8 = 0
            let count = inputStream.read(&char, maxLength: 1)
            
            assert(count > 0)
            
            bytesProcessed++
            
            if foundCR == true && char == charLF {
                done = true
            } else if ( char == charCR ) {
                foundCR = true
            }
        }
        
        if !done {
            return nil
        }
        
        let result = getNextDataOfSize(bytesProcessed-2)
        removeBytesFromBuffer(2) // remove the CRLF
        
        return result
        
    }
    
    func storeReceivedBytes(data: NSData)
    {
        if dataAccumulatedFromStream == nil {
            dataAccumulatedFromStream = data
        } else {
            let existingData = dataAccumulatedFromStream!
            let mutableData = NSMutableData.init(data: existingData)
            mutableData.appendData(data)
            dataAccumulatedFromStream = mutableData
        }
    }
    
    func storeReceivedString(string: String)
    {
        let data = string.dataUsingEncoding(NSUTF8StringEncoding)
        
        storeReceivedBytes(data!)
    }
    
}
