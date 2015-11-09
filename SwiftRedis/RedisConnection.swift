//
//  ConnectionToRedis.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/1/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import UIKit



protocol RedisConnectionDelegate {
    func connected()
    func connectionError(error: String)
}


class RedisConnection : NSObject, NSStreamDelegate, RedisResponseParserDelegate
{
    // MARK: init
    
    let serverAddress: CFString
    let serverPort: UInt32
    let enableSSL = false

    init(serverAddress: String, serverPort: UInt32)
    {
        self.serverAddress = serverAddress
        self.serverPort = serverPort
    }
    
    // account on redislabs.com
    
    
    // MARK: Delegate
    var delegate: RedisConnectionDelegate?
    
    func setDelegate(delegate: RedisConnectionDelegate)
    {
        self.delegate = delegate
    }
    
    // MARK: Streams
    private var inputStream: NSInputStream?
    private var outputStream: NSOutputStream?
    
    func statusRequiresOpening(status: CFStreamStatus) -> Bool {
        switch(status) {
        case .Closed, .Error, .NotOpen: return true
        case .AtEnd, .Open, .Opening, .Reading, .Writing: return false
        }
        
    }
    
    func inputStreamRequiresOpening(stream: NSInputStream?) -> Bool
    {
        if stream == nil { return true }
        let isStatus = CFReadStreamGetStatus(stream)
        return statusRequiresOpening(isStatus)
    }
    
    func outputStreamRequiresOpening(stream: NSOutputStream?) -> Bool
    {
        if stream == nil { return true }
        let osStatus = CFWriteStreamGetStatus(stream)
        return statusRequiresOpening(osStatus)
    }
    
    func disconnect() {
        connectionState = .Closed

        if inputStream != nil {
            inputStream!.close()
            print("closed input stream. status is now: \(statusOfStreamAsString(inputStream))")
            inputStream = nil
        }
        if outputStream != nil {
            outputStream!.close()
            print("closed output stream. status is now: \(statusOfStreamAsString(outputStream))")
            outputStream = nil
        }
    }
    
    deinit {
        disconnect()
    }
    
    func connect() {
        
        print("input stream: \(statusOfStreamAsString(inputStream))   output stream: \(statusOfStreamAsString(outputStream))")
        
        if !inputStreamRequiresOpening(self.inputStream) && !outputStreamRequiresOpening(self.outputStream) {
            print("Streams are already open.  No need to reconnect")
            return
        } else {
            disconnect()
        }
        
        print("connecting...")
        
        var readStream:  Unmanaged<CFReadStream>?
        var writeStream: Unmanaged<CFWriteStream>?
        
        CFStreamCreatePairWithSocketToHost(nil, self.serverAddress, self.serverPort, &readStream, &writeStream)
        
        // Documentation suggests readStream and writeStream can be assumed to
        // be non-nil. If you believe otherwise, you can test if either is nil
        // and implement whatever error-handling you wish.
        
        self.inputStream = readStream!.takeRetainedValue()
        self.outputStream = writeStream!.takeRetainedValue()
        
        self.inputStream!.delegate = self
        self.outputStream!.delegate = self
        
        self.inputStream!.scheduleInRunLoop(NSRunLoop.currentRunLoop(), forMode: NSDefaultRunLoopMode)
        self.outputStream!.scheduleInRunLoop(NSRunLoop.currentRunLoop(), forMode: NSDefaultRunLoopMode)

        if enableSSL {
            self.inputStream?.setProperty(NSStreamSocketSecurityLevelNegotiatedSSL, forKey: NSStreamSocketSecurityLevelKey)
            self.outputStream?.setProperty(NSStreamSocketSecurityLevelNegotiatedSSL, forKey: NSStreamSocketSecurityLevelKey)
        }
        
        self.inputStream!.open()
        self.outputStream!.open()
    }

    // keep track of the connection to the web service
    enum ConnectionState { case Closed, Ready, Error }
    var connectionState: ConnectionState = .Closed
    
    func stream(stream: NSStream, handleEvent eventCode: NSStreamEvent) {
        
        if stream == inputStream { handleInputStreamEvent(eventCode) }
        else if stream == outputStream { handleOutputStreamEvent(eventCode) }
        else {
            assert(false)
        }
    }

    
    func readData(maxBytes: Int = 128) -> NSData?
    {
        let data = NSMutableData(length: maxBytes)!
        
        print("readData: reading up to \(maxBytes) from stream")
        let length = inputStream?.read(UnsafeMutablePointer<UInt8>(data.mutableBytes), maxLength: maxBytes)
        print("readData: read \(length) bytes")
        if length == nil {
            return nil
        }
        
        data.length = length!
        
        return data
    }
    
    
    func error(message: String)
    {
        connectionState = .Error
        delegate?.connectionError(message)
    }
    

    let responseParser = RedisResponseParser()
    
    func handleInputStreamEvent(eventCode: NSStreamEvent)
    {
        responseParser.setDelegate(self)
        
        switch(eventCode)
        {
        case NSStreamEvent.OpenCompleted:
            // nothing to do here
            print("input stream: .OpenCompleted")
        case NSStreamEvent.HasBytesAvailable:
            print("input stream: .HasBytesAvaialable")
            switch connectionState {
            case .Closed:
                warnIf(true, "InputStream - HasBytesAvailable when state is .Closed")
            case .Ready:
                while inputStream!.hasBytesAvailable {
                    let data = readData()
                    if data != nil {
                        responseParser.storeReceivedData(data!)
                    } else {
                        warn("could not read data even though inputStream!.hasBytesAvaialble is true")
                    }
                }
            case .Error:
                break
            }
            
        case NSStreamEvent.EndEncountered:
            print("input stream: .EndEncountered")
        case NSStreamEvent.ErrorOccurred:
            print("input stream: .ErrorEncountered")
        case NSStreamEvent.HasSpaceAvailable:
            print("input stream: .HasSpaceAvailable")
        case NSStreamEvent.None:
            print("input stream: .None")
        default:
            print("input stream: unknown event \(eventCode)")
        }
    }
    
    func warn(description: String)
    {
        NSLog("**** WARNING:  \(description)")
    }
    
    func warnIf(condition: Bool, _ description: String) {
        if !condition { return }
        
        warn(description)
    }

    // MARK: Response Parser Delegate
    func errorParsingResponse(error: String) {
        delegate?.connectionError("Could not understand response received")
    }
    
    func receivedResponse(response: RedisResponse) {
        if let cmd = pendingCommand {
            if cmd.finishWhenResponseReceived {
                pendingCommand = nil
            }
            cmd.responseReceived(response)
        } else {
            warn("Response received when no command pending \(response)")
        }
        
    }

    
    func handleOutputStreamEvent(eventCode: NSStreamEvent)
    {
        switch(eventCode)
        {
        case NSStreamEvent.OpenCompleted:
            warnIf(connectionState != .Closed, "OutputStream .OpenCompleted when connectionState is \(connectionState)")
            
            // mark the current state as "Unauthenticated", so that when the 
            // output stream is ready for writing, we'll send the authentication command
            connectionState = .Ready
            delegate?.connected()
            
        case NSStreamEvent.HasSpaceAvailable:
            
            // the action to take depends on the state
            switch connectionState {
            case .Closed:
                warn("OutputStream - HasSpaceAvailable when state is .Closed")

            case .Ready:
                sendPendingDataIfPossible()
                
            case .Error:
                assert(false)
            }
        case NSStreamEvent.OpenCompleted:
            // nothing to do here
            print("output stream: .OpenCompleted")
        case NSStreamEvent.HasBytesAvailable:
            print("output stream: .HasBytesAvaialable")
        case NSStreamEvent.EndEncountered:
            print("output stream: .EndEncountered")
        case NSStreamEvent.ErrorOccurred:
            print("output stream: .ErrorEncountered")
        case NSStreamEvent.HasSpaceAvailable:
            print("output stream: .HasSpaceAvailable")
            sendPendingDataIfPossible()
        case NSStreamEvent.None:
            print("output stream: .None")
        default:
            warn("output stream: unknown event \(eventCode)")
        }
    }

    var pendingCommand: RedisCommand? = nil
    
    
    // this function should be called in two cases:
    // 1 - the stream has become ready for writing
    // 2 - new command is available for sending
    func sendPendingDataIfPossible()
    {
        if pendingCommand?.sent == true { return }
        
        if let command = pendingCommand {
            
            if outputStream?.hasSpaceAvailable == true {
                if let data = command.getCommandString() {
                    command.sent = true
                    print("sending command string: \(String(data: data, encoding: NSUTF8StringEncoding))")
                    outputStream?.write(UnsafePointer<UInt8>(data.bytes), maxLength: data.length)
                } else {
                    warn("could not get command string from pendingCommand")
                }
            } else {
                print("sendPendingDataIfPossible: not sending data because outputStream.hasSpaceAvaialble is false")
            }
        }
    }

    
    // MARK: Redis Commands
    
    
    func setPendingCommand(command: RedisCommand)
    {
        assert(pendingCommand == nil)
       
        pendingCommand = command
        sendPendingDataIfPossible()
    }
    
}




// MARK: Utility functions for debugging

func statusAsString(status: NSStreamStatus) -> String {
    switch(status) {
    case .Closed: return "Closed"
    case .AtEnd: return "AtEnd"
    case .Error: return "Error"
    case .NotOpen: return "NotOpen"
    case .Open: return "Open"
    case .Opening: return "Opening"
    case .Reading: return "Reading"
    case .Writing: return "Writing"
    }
}

func statusAsString(status: CFStreamStatus) -> String {
    switch(status) {
    case .Closed: return "Closed"
    case .AtEnd: return "AtEnd"
    case .Error: return "Error"
    case .NotOpen: return "NotOpen"
    case .Open: return "Open"
    case .Opening: return "Opening"
    case .Reading: return "Reading"
    case .Writing: return "Writing"
    }
}

func statusOfStreamAsString(stream: NSStream?) -> String
{
    if stream == nil { return "<stream is nil>" }
    return statusAsString(stream!.streamStatus)
}

