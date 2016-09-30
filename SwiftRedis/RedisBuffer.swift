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
    var dataAccumulatedFromStream: Data?
    
    func clear()
    {
        dataAccumulatedFromStream = nil
    }
    
    func restoreRemovedData(_ dataToRestore: Data) {
        if dataAccumulatedFromStream == nil {
            dataAccumulatedFromStream = dataToRestore
            return
        } else {
            var combinedData = NSData(data: dataToRestore) as Data
            combinedData.append(dataAccumulatedFromStream!)
            dataAccumulatedFromStream = combinedData
        }
    }
    
    func removeBytesFromBuffer(_ numBytesToRemove: Int)
    {
        let data = dataAccumulatedFromStream!.subdata(in: numBytesToRemove ..< dataAccumulatedFromStream!.count)

        if data.count == 0 {
            dataAccumulatedFromStream = nil
        } else {
            dataAccumulatedFromStream = data
        }
    }
    
/* Swift 2 syntax:
    func removeBytesFromBuffer(_ numBytesToRemove: Int)
    {
        let len = dataAccumulatedFromStream!.count - numBytesToRemove
        
        let range = NSMakeRange(numBytesToRemove, len)
        let data = dataAccumulatedFromStream!.subdata(in: range)
        
        if data.count == 0 {
            dataAccumulatedFromStream = nil
        } else {
            dataAccumulatedFromStream = data
        }
    }
*/
    
    func getNextDataOfSize(_ size: Int?) -> Data?
    {
        if size == nil { return getNextDataUntilCRLF() }
        
        if dataAccumulatedFromStream == nil { return nil }
        if dataAccumulatedFromStream!.count < size! {
            return nil
        }
        
        let data = dataAccumulatedFromStream!.subdata(in: 0 ..< size! )
        
        removeBytesFromBuffer(size!)
        
        return data
    }
    
/* Swift 2 syntax:
    func getNextDataOfSize(_ size: Int?) -> Data?
    {
        if size == nil { return getNextDataUntilCRLF() }
        
        if dataAccumulatedFromStream == nil { return nil }
        if dataAccumulatedFromStream!.count < size! {
            return nil
        }
        
        let range = NSMakeRange(0, size!)
        let data = dataAccumulatedFromStream!.subdata(in: range)
        
        removeBytesFromBuffer(size!)
        
        return data
    }
*/
    
    func getNextStringOfSize(_ size: Int) -> String?
    {
        let data = getNextDataOfSize(size)
        if data == nil { return nil }
        
        return String(NSString(data: data!, encoding: String.Encoding.utf8.rawValue)!)
    }
    
    func getNextStringUntilCRLF() -> String?
    {
        let data = getNextDataUntilCRLF()
        if data == nil { return nil }
        
        return String(NSString(data: data!, encoding: String.Encoding.utf8.rawValue)!)
    }
    
    func getNextDataUntilCRLF() -> Data?
    {
        if dataAccumulatedFromStream == nil { return nil }
        
        let len = dataAccumulatedFromStream!.count
        
        var done = false
        var bytesProcessed = 0
        
        let charCR = UInt8(0x0d)
        let charLF = UInt8(0x0a)
        
        var foundCR = false
        
        let inputStream = InputStream(data: dataAccumulatedFromStream!)
        inputStream.open()
        
        while !done && bytesProcessed < len
        {
            var char: UInt8 = 0
            let count = inputStream.read(&char, maxLength: 1)
            
            assert(count > 0)
            
            bytesProcessed += 1
            
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
    
    func storeReceivedBytes(_ data: Data)
    {
        if dataAccumulatedFromStream == nil {
            dataAccumulatedFromStream = data
        } else {
            let existingData = dataAccumulatedFromStream!
            var mutableData = NSData.init(data: existingData) as Data
            mutableData.append(data)
            dataAccumulatedFromStream = mutableData
        }
    }
    
    func storeReceivedString(_ string: String)
    {
        let data = string.data(using: String.Encoding.utf8)
        
        storeReceivedBytes(data!)
    }
    
}
