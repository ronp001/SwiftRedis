//
//  ConnectionParams.swift
//  RedisConnection
//
//  Created by Ron Perry on 11/8/15.
//  Copyright © 2015 Ron Perry. All rights reserved.
//

import Foundation

// NOTE:  This file only serves as a template, and is not referenced by the unit tests.
//
// To run the unit tests:
//
// 1. Rename the class ConnectionParamsExample to ConnectionParams
// 2. Save a copy of this file as ../../ConnectionParams.swift
// 3. Put the real connection parameters (those that you don't want to share publicly) in ConnectionParams.swift
// 4. Run the unit tests
//
class ConnectionParamsExample {
    static let serverAddress = "localhost"
    static let serverPort = UInt32(6379)
    static let auth = "connection-secret"  // warning:  including the authentication secret as an unecrypted string in an iOS app is extremely unsecure:  hackers can easily grab this string from the app.  Use this method only for unit testing, or if the app is installed only on trusted devices.
}
