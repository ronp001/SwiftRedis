//
//  RedisResponseParser.swift
//  ActivityKeeper
//
//  Created by Ron Perry on 11/7/15.
//  Copyright © 2015 ronp001. All rights reserved.
//

import Foundation


protocol RedisResponseParserDelegate {
    func errorParsingResponse(_ error: String?)
    func receivedResponse(_ response: RedisResponse)
    func parseOperationAborted()
}


class RedisResponseParser: RedisResponseParserDelegate
{
    // MARK: The Response
    fileprivate var _haveResponse = false
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
    
    func setDelegate(_ delegate: RedisResponseParserDelegate)
    {
        self.delegate = delegate
    }
    
    func error(_ message: String?) {
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
    
    func receivedResponse(_ response: RedisResponse) {
        arrayElementsLeft! -= 1
        
        self.response!.addArrayElement(response)
        
        if arrayElementsLeft == 0 {
            detachSubParser()
            finishProcessing(.success, errorMessage: nil)
        }
    }
    
    func errorParsingResponse(_ error: String?) {
        finishProcessing(.failure, errorMessage: error)
    }
    
    func parseOperationAborted() {
        finishProcessing(.aborted, errorMessage: "Aborted")
    }
    
    
    // MARK: Processing data accumulated from stream
    enum ParserState { case waitingForTypeIndicator, waitingForSizeIndicator, waitingForData, waitingForArrayElementCount, processingArrayElements, idle }
    
    var parserState: ParserState = .idle
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
            case .waitingForTypeIndicator, .idle:
                response = nil
                self._haveResponse = false
                
                let typeChar = redisBuffer.getNextStringOfSize(1)
                if typeChar == nil { return false }
                
                print("read type char: '\(typeChar)'")
                switch typeChar! as String {
                case "$":   // Bulk String
                    response = RedisResponse(responseType: .data)
                    expectingNumOfBytes = 0  // will be set by the size indicator
                    parserState = .waitingForSizeIndicator
                    
                case ":":   // Integer
                    response = RedisResponse(responseType: .int)
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .waitingForData
                    
                case "+":   // Simple String
                    response = RedisResponse(responseType: .string)
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .waitingForData
                    
                case "-":   // Error
                    response = RedisResponse(responseType: .error)
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .waitingForData
                    
                case "*":   // Array
                    response = RedisResponse(responseType: .array)
                    arrayElementsLeft = nil
                    expectingNumOfBytes = nil  // read until CRLF
                    parserState = .waitingForArrayElementCount
                    
                default:
                    error("unexpected character received while expecting type char: '\(typeChar)'")
                }
                
            case .waitingForSizeIndicator:
                let sizeStr = redisBuffer.getNextStringUntilCRLF()
                
                if sizeStr == nil { return false }
                
                let size = Int(sizeStr!)
                
                if size == nil {
                    error("Expected size indicator.  Received \(sizeStr)")
                    parserState = .waitingForTypeIndicator
                    return false
                }
                
                expectingNumOfBytes = size
                parserState = .waitingForData
                

            case .waitingForArrayElementCount:
                let sizeStr = redisBuffer.getNextStringUntilCRLF()
                if sizeStr == nil { return false }
                let size = Int(sizeStr!)
                
                if size == nil {
                    error("Expected size indicator.  Received \(sizeStr)")
                    parserState = .waitingForTypeIndicator
                    return false
                }
                arrayElementsLeft = size!
                print("Expecting \(size) elements in array")
                parserState = .processingArrayElements
                deploySubParser()
                
            case .processingArrayElements:
                // for arrays, we need to instanciate a new parser and pipe data to it
                // the sub-parser uses the same buffer as the parent parser.
                if subParser == nil {
                    deploySubParser()
                }
                
                // the following call will activate the delegate functions (receivedResponse or errorParsing Response) when complete
                let subParserSuccess = subParser!.processAccumulatedData()
                
                if !subParserSuccess { return false }
                
            case .waitingForData:
                let success = response!.readValueFromBuffer(redisBuffer, numBytes: expectingNumOfBytes)
                
                if success == nil { return false }

                finishProcessing(success! ? .success : .failure, errorMessage: nil)
            }
        }
        return true
    }
    
    enum ParseOperationCompletionStatus { case success, failure, aborted }
    
    func finishProcessing(_ status: ParseOperationCompletionStatus, errorMessage: String?)
    {
        processingComplete = true
        parserState = .idle
        
        // inform the delegate that the read operation was completed.
        // note:  when the delegate is called, it might initiate a recursive call to this function!
        switch status {
        case .success:
            self._haveResponse = true
            self.delegate?.receivedResponse(response!)
        case .failure:
            self.delegate?.errorParsingResponse(errorMessage)
        case .aborted:
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
        parserState = .idle
        aborted()
    }
    
    
    
    // MARK: Reading from stream
    func storeReceivedData(_ data: Data)
    {
        redisBuffer.storeReceivedBytes(data)
        processAccumulatedData()
    }
    
    func storeReceivedString(_ str: String)
    {
        storeReceivedData(str.data(using: String.Encoding.utf8)!)
    }
}
