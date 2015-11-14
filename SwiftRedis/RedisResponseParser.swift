//
//  RedisResponseParser.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation


protocol RedisResponseParserDelegate {
    func errorParsingResponse(error: String?)
    func receivedResponse(response: RedisResponse)
    func parseOperationAborted()
}


class RedisResponseParser: RedisResponseParserDelegate
{
    // MARK: The Response
    private var _haveResponse = false
    var haveResponse: Bool {
        get { return _haveResponse }
    }
    
    var lastResponse: RedisResponse? {
        get { return response }
    }

    
    // MARK: Initialization
    let redisBuffer: RedisBuffer
    
    init(redisBuffer: RedisBuffer? = nil)
    {
        self.redisBuffer = redisBuffer ?? RedisBuffer()
    }
    

    // MARK: Calling the Delegate
    var delegate: RedisResponseParserDelegate?
    
    func setDelegate(delegate: RedisResponseParserDelegate)
    {
        self.delegate = delegate
    }
    
    func error(message: String?) {
        self.delegate?.errorParsingResponse(message)
    }
    
    func aborted() {
        self.delegate?.parseOperationAborted()
    }
    
    
    // MARK: Array processing
    var subParser: RedisResponseParser? = nil

    func deploySubParser()
    {
        subParser = RedisResponseParser(redisBuffer: self.redisBuffer)
        subParser?.setDelegate(self)
    }
    
    func detachSubParser()
    {
        // remove the subParser
        subParser = nil
    }
    
    // MARK:  acting as the delegate for the sub-parser (when processing arrays)
    
    func receivedResponse(response: RedisResponse) {
        arrayElementsLeft!--
        
        self.response!.addArrayElement(response)
        
        if arrayElementsLeft == 0 {
            detachSubParser()
            finishProcessing(.Success, errorMessage: nil)
        }
    }
    
    func errorParsingResponse(error: String?) {
        finishProcessing(.Failure, errorMessage: error)
    }
    
    func parseOperationAborted() {
        finishProcessing(.Aborted, errorMessage: "Aborted")
    }
    
    
    // MARK: Processing data accumulated from stream
    enum ParserState { case WaitingForTypeIndicator, WaitingForSizeIndicator, WaitingForData, WaitingForArrayElementCount, ProcessingArrayElements, Idle }
    
    var parserState: ParserState = .Idle
    var expectingNumOfBytes: Int? // if nil:  read until CRLF
    var response: RedisResponse?
    
    // MARK: for array processing
    var arrayElementsLeft: Int?
    
    
    var processingComplete = false
    
    // function returns true if managed to complete processing, false otherwise
    func processAccumulatedData() -> Bool
    {
        processingComplete = false
        
        while !processingComplete
        {
            switch parserState {
            case .WaitingForTypeIndicator, .Idle:
                response = nil
                self._haveResponse = false
                
                let typeChar = redisBuffer.getNextStringOfSize(1)
                if typeChar == nil { return false }
                
                print("read type char: '\(typeChar)'")
                switch typeChar! as String {
                case "$":   // Bulk String
                    response = RedisResponse(responseType: .Data)
                    expectingNumOfBytes = 0  // will be set by the size indicator
                    parserState = .WaitingForSizeIndicator
                    
                case ":":   // Integer
                    response = RedisResponse(responseType: .Int)
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .WaitingForData
                    
                case "+":   // Simple String
                    response = RedisResponse(responseType: .String)
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .WaitingForData
                    
                case "-":   // Error
                    response = RedisResponse(responseType: .Error)
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .WaitingForData
                    
                case "*":   // Array
                    response = RedisResponse(responseType: .Array)
                    arrayElementsLeft = nil
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .WaitingForArrayElementCount
                    
                default:
                    error("unexpected character received while expecting type char: '\(typeChar)'")
                }
                
            case .WaitingForSizeIndicator:
                let sizeStr = redisBuffer.getNextStringUntilCRLF()
                
                if sizeStr == nil { return false }
                
                let size = Int(sizeStr!)
                
                if size == nil {
                    error("Expected size indicator.  Received \(sizeStr)")
                    parserState = .WaitingForTypeIndicator
                    return false
                }
                
                expectingNumOfBytes = size
                parserState = .WaitingForData
                

            case .WaitingForArrayElementCount:
                let sizeStr = redisBuffer.getNextStringUntilCRLF()
                if sizeStr == nil { return false }
                let size = Int(sizeStr!)
                
                if size == nil {
                    error("Expected size indicator.  Received \(sizeStr)")
                    parserState = .WaitingForTypeIndicator
                    return false
                }
                arrayElementsLeft = size!
                print("Expecting \(size) elements in array")
                parserState = .ProcessingArrayElements
                deploySubParser()
                
            case .ProcessingArrayElements:
                // for arrays, we need to instanciate a new parser and pipe data to it
                // the sub-parser uses the same buffer as the parent parser.
                if subParser == nil {
                    deploySubParser()
                }
                
                // the following call will activate the delegate functions (receivedResponse or errorParsing Response) when complete
                let subParserSuccess = subParser!.processAccumulatedData()
                
                if !subParserSuccess { return false }
                
            case .WaitingForData:
                let success = response!.readValueFromBuffer(redisBuffer, numBytes: expectingNumOfBytes)
                
                if success == nil { return false }

                finishProcessing(success! ? .Success : .Failure, errorMessage: nil)
            }
        }
        return true
    }
    
    enum ParseOperationCompletionStatus { case Success, Failure, Aborted }
    
    func finishProcessing(status: ParseOperationCompletionStatus, errorMessage: String?)
    {
        processingComplete = true
        parserState = .Idle
        
        // inform the delegate that the read operation was completed.
        // note:  when the delegate is called, it might initiate a recursive call to this function!
        switch status {
        case .Success:
            self._haveResponse = true
            self.delegate?.receivedResponse(response!)
        case .Failure:
            self.delegate?.errorParsingResponse(errorMessage)
        case .Aborted:
            self.delegate?.parseOperationAborted()
        }
    }
    
    // MARK:  Aborting
    func abortParsing()
    {
        if subParser != nil {
            subParser?.abortParsing()
            subParser = nil
        }
        redisBuffer.clear()
        response = nil
        _haveResponse = false
        parserState = .Idle
        aborted()
    }
    
    
    
    // MARK: Reading from stream
    func storeReceivedData(data: NSData)
    {
        redisBuffer.storeReceivedBytes(data)
        processAccumulatedData()
    }
    
    func storeReceivedString(str: String)
    {
        storeReceivedData(str.dataUsingEncoding(NSUTF8StringEncoding)!)
    }
}
