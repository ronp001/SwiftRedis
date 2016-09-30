//
//  ConnectionToRedis.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/1/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import UIKit
fileprivate func < <T : Comparable>(lhs: T?, rhs: T?) -> Bool {
  switch (lhs, rhs) {
  case let (l?, r?):
    return l < r
  case (nil, _?):
    return true
  default:
    return false
  }
}




protocol RedisConnectionDelegate {
    func connected()
    func connectionError(_ error: String)
}


class RedisConnection : NSObject, StreamDelegate, RedisResponseParserDelegate
{
    // MARK: init
    
    let serverAddress: CFString
    let serverPort: UInt32
    let enableSSL = false

    init(serverAddress: String, serverPort: UInt32)
    {
        self.serverAddress = serverAddress as CFString
        self.serverPort = serverPort
    }
    
    // account on redislabs.com
    
    
    // MARK: Delegate
    var delegate: RedisConnectionDelegate?
    
    func setDelegate(_ delegate: RedisConnectionDelegate)
    {
        self.delegate = delegate
    }
    
    // MARK: Streams
    fileprivate var inputStream: InputStream?
    fileprivate var outputStream: OutputStream?
    
    func statusRequiresOpening(_ status: CFStreamStatus) -> Bool {
        switch(status) {
        case .closed, .error, .notOpen: return true
        case .atEnd, .open, .opening, .reading, .writing: return false
        }
        
    }
    
    func inputStreamRequiresOpening(_ stream: InputStream?) -> Bool
    {
        if stream == nil { return true }
        let isStatus = CFReadStreamGetStatus(stream)
        return statusRequiresOpening(isStatus)
    }
    
    func outputStreamRequiresOpening(_ stream: OutputStream?) -> Bool
    {
        if stream == nil { return true }
        let osStatus = CFWriteStreamGetStatus(stream)
        return statusRequiresOpening(osStatus)
    }
    
    func disconnect() {
        connectionState = .closed

        if inputStream != nil {
            inputStream!.close()
            print("closed input stream. status is now: \(statusOfStreamAsString(inputStream))")
            //inputStream = nil
        }
        if outputStream != nil {
            outputStream!.close()
            print("closed output stream. status is now: \(statusOfStreamAsString(outputStream))")
            //outputStream = nil
        }

        if pendingCommand != nil {
            pendingCommand!.completionFailed()
            pendingCommand = nil
        }

        responseParser.abortParsing()
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
        
        self.inputStream!.schedule(in: RunLoop.current, forMode: RunLoopMode.defaultRunLoopMode)
        self.outputStream!.schedule(in: RunLoop.current, forMode: RunLoopMode.defaultRunLoopMode)

        if enableSSL {
            self.inputStream?.setProperty(StreamSocketSecurityLevel.negotiatedSSL, forKey: Stream.PropertyKey.socketSecurityLevelKey)
            self.outputStream?.setProperty(StreamSocketSecurityLevel.negotiatedSSL, forKey: Stream.PropertyKey.socketSecurityLevelKey)
        }
        
        self.inputStream!.open()
        self.outputStream!.open()
    }

    // keep track of the connection to the web service
    enum ConnectionState { case closed, ready, error }
    var connectionState: ConnectionState = .closed
    
    func stream(_ stream: Stream, handle eventCode: Stream.Event) {
        
        if stream == inputStream { handleInputStreamEvent(eventCode) }
        else if stream == outputStream { handleOutputStreamEvent(eventCode) }
        else {
            assert(false)
        }
    }

    
    func readData(_ maxBytes: Int = 1024) -> Data?
    {
        let data = NSMutableData(length: maxBytes)!
        
        print("readData: reading up to \(maxBytes) from stream")
        let length = inputStream?.read(UnsafeMutablePointer<UInt8>(data.mutableBytes), maxLength: maxBytes)
        print("readData: read \(length) bytes")
        if length == nil || length < 0 {
            return nil
        }
        
        data.length = length!
        
        return data as Data
    }
    
    
    func error(_ message: String)
    {
        connectionState = .error
        delegate?.connectionError(message)
    }
    

    let responseParser = RedisResponseParser()
    
    func handleInputStreamEvent(_ eventCode: Stream.Event)
    {
        responseParser.setDelegate(self)
        
        switch(eventCode)
        {
        case Stream.Event.openCompleted:
            // nothing to do here
            print("input stream: .OpenCompleted")
        case Stream.Event.hasBytesAvailable:
            print("input stream: .HasBytesAvaialable")
            switch connectionState {
            case .closed:
                warnIf(true, "InputStream - HasBytesAvailable when state is .Closed")
            case .ready:
                while inputStream!.hasBytesAvailable {
                    let data = readData()
                    if data != nil {
                        responseParser.storeReceivedData(data!)
                    } else {
                        warn("could not read data even though inputStream!.hasBytesAvaialble is true")
                    }
                }
            case .error:
                break
            }
            
        case Stream.Event.endEncountered:
            print("input stream: .EndEncountered")
        case Stream.Event.errorOccurred:
            print("input stream: .ErrorEncountered")
        case Stream.Event.hasSpaceAvailable:
            print("input stream: .HasSpaceAvailable")
        case Stream.Event():
            print("input stream: .None")
        default:
            print("input stream: unknown event \(eventCode)")
        }
    }
    
    func warn(_ description: String)
    {
        NSLog("**** WARNING:  \(description)")
    }
    
    func warnIf(_ condition: Bool, _ description: String) {
        if !condition { return }
        
        warn(description)
    }

    // MARK: Response Parser Delegate
    func errorParsingResponse(_ error: String?) {
        var message = "Could not parse response received"
        if error != nil {
            message += "(" + error! + ")"
        }
        delegate?.connectionError(message)
    }
    
    func parseOperationAborted() {
        print("RedisConnection: parse operation aborted")
    }
    
    func receivedResponse(_ response: RedisResponse) {
        if let cmd = pendingCommand {
            if cmd.finishWhenResponseReceived {
                pendingCommand = nil
            }
            cmd.responseReceived(response)
        } else {
            warn("Response received when no command pending \(response)")
        }
        
    }

    
    func handleOutputStreamEvent(_ eventCode: Stream.Event)
    {
        switch(eventCode)
        {
        case Stream.Event.openCompleted:
            warnIf(connectionState != .closed, "OutputStream .OpenCompleted when connectionState is \(connectionState)")
            
            // mark the current state as "Unauthenticated", so that when the 
            // output stream is ready for writing, we'll send the authentication command
            connectionState = .ready
            delegate?.connected()
            
        case Stream.Event.hasSpaceAvailable:
            
            // the action to take depends on the state
            switch connectionState {
            case .closed:
                warn("OutputStream - HasSpaceAvailable when state is .Closed")

            case .ready:
                sendPendingDataIfPossible()
                
            case .error:
                assert(false)
            }
        case Stream.Event.openCompleted:
            // nothing to do here
            print("output stream: .OpenCompleted")
        case Stream.Event.hasBytesAvailable:
            print("output stream: .HasBytesAvaialable")
        case Stream.Event.endEncountered:
            print("output stream: .EndEncountered")
        case Stream.Event.errorOccurred:
            print("output stream: .ErrorEncountered")
        case Stream.Event.hasSpaceAvailable:
            print("output stream: .HasSpaceAvailable")
            sendPendingDataIfPossible()
        case Stream.Event():
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
                    print("sending command string: \(String(data: data as Data, encoding: String.Encoding.utf8))")
                    outputStream?.write((data as NSData).bytes.bindMemory(to: UInt8.self, capacity: data.count), maxLength: data.count)
                } else {
                    warn("could not get command string from pendingCommand")
                }
            } else {
                print("sendPendingDataIfPossible: not sending data because outputStream.hasSpaceAvaialble is false")
            }
        }
    }

    
    // MARK: Redis Commands
    
    
    func setPendingCommand(_ command: RedisCommand)
    {
        assert(pendingCommand == nil)
       
        pendingCommand = command
        sendPendingDataIfPossible()
    }
    
}




// MARK: Utility functions for debugging

func statusAsString(_ status: Stream.Status) -> String {
    switch(status) {
    case .closed: return "Closed"
    case .atEnd: return "AtEnd"
    case .error: return "Error"
    case .notOpen: return "NotOpen"
    case .open: return "Open"
    case .opening: return "Opening"
    case .reading: return "Reading"
    case .writing: return "Writing"
    }
}

func statusAsString(_ status: CFStreamStatus) -> String {
    switch(status) {
    case .closed: return "Closed"
    case .atEnd: return "AtEnd"
    case .error: return "Error"
    case .notOpen: return "NotOpen"
    case .open: return "Open"
    case .opening: return "Opening"
    case .reading: return "Reading"
    case .writing: return "Writing"
    }
}

func statusOfStreamAsString(_ stream: Stream?) -> String
{
    if stream == nil { return "<stream is nil>" }
    return statusAsString(stream!.streamStatus)
}

