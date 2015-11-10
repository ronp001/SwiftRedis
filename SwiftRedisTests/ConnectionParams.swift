//
//  ConnectionParams.swift
//  RedisConnection
//
//  Created by Ron Perry on 11/8/15.
//  Copyright © 2015 Ron Perry. All rights reserved.
//

import Foundation

class ConnectionParams {
    static let serverAddress = "localhost1"
    static let serverPort = UInt32(6379)
    static let auth = "connection-secret"  // warning:  including the authentication secret as an unecrypted string in an iOS app is extremely unsecure:  hackers can easily grab this string from the app.  Use this method only for the purpose of Unit Testing or if the app is distributed only to trusted devices.
}
