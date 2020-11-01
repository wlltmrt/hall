//
//  Hall
//
//  Copyright (c) 2020 Wellington Marthas
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.
//

import Foundation
import Adrenaline
import SQLCipher

public enum DatabaseError: Error {
    case invalidQuery(query: String, description: String)
    case unknown(description: String)
}

public final class DatabaseConnection {
    public enum Location {
        case memory
        case file(fileName: String)
    }
    
    private static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    private var databaseHandle: OpaquePointer?
    
    init(location: Location, key: String) throws {
        let path: String
        
        switch location {
        case let .file(fileName):
            path = FileManager.default.inApplicationSupportDirectory(with: fileName).path
            
        case .memory:
            path = ":memory:"
        }
        
        if let databaseHandle = databaseHandle {
            sqlite3_close_v2(databaseHandle)
        }
        
        if sqlite3_open_v2(path, &databaseHandle, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX, nil) != SQLITE_OK {
            Database.default.log.debug("🔶 ERROR: Can't open database")
            throw DatabaseError.firstChance(.unknown(description: "Can't open database: \(path)"))
        }
        
        try exec(query: "PRAGMA cipher_memory_security=OFF")
        sqlite3_key(databaseHandle, key, Int32(key.utf8.count))
    }
    
    deinit {
        sqlite3_close_v2(databaseHandle)
        databaseHandle = nil
    }
    
    func exec(query: String) throws {
        if sqlite3_exec(databaseHandle, query, nil, nil, nil) == SQLITE_ERROR {
            throw unknownError()
        }
    }
    
    func execute(_ query: Query) throws {
        var statementHandle: OpaquePointer? = nil
        var result: CInt = 0
        
        if let values = query.values {
            try prepare(to: &statementHandle, query: query.query)
            
            let bindCount = sqlite3_bind_parameter_count(statementHandle)
            
            for i in 1...bindCount {
                try bind(to: statementHandle, value: values[Int(i) - 1], at: i)
            }
            
            step(to: statementHandle, result: &result)
            sqlite3_finalize(statementHandle)
            
        }
        else {
            result = sqlite3_exec(databaseHandle, query.query, nil, nil, nil)
        }
        
        if result == SQLITE_ERROR {
            throw unknownError()
        }
    }
    
    func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T, using block: (T) -> Void) throws {
        var statementHandle: OpaquePointer? = nil
        try prepare(to: &statementHandle, query: query.query)
        
        if let values = query.values {
            let bindCount = sqlite3_bind_parameter_count(statementHandle)
            
            for i in 1...bindCount {
                try bind(to: statementHandle, value: values[Int(i) - 1], at: i)
            }
        }
        
        let statement = Statement(handle: statementHandle)
        var result: CInt = 0
        
        while step(to: statementHandle, result: &result) {
            block(adaptee(statement))
        }
        
        sqlite3_finalize(statementHandle)
        
        if result == SQLITE_ERROR {
            throw unknownError()
        }
    }
    
    func scalar<T>(query: Query, adaptee: (_ statement: Statement) -> T?) throws -> T? {
        var statementHandle: OpaquePointer? = nil
        try prepare(to: &statementHandle, query: query.query)
        
        if let values = query.values {
            let bindCount = sqlite3_bind_parameter_count(statementHandle)
            
            for i in 1...bindCount {
                try bind(to: statementHandle, value: values[Int(i) - 1], at: i)
            }
        }
        
        var result: CInt = 0
        var item: T?
        
        if step(to: statementHandle, result: &result) {
            item = adaptee(Statement(handle: statementHandle))
        }
        
        sqlite3_finalize(statementHandle)
        
        if result == SQLITE_ERROR {
            throw unknownError()
        }
        
        return item
    }
    
    private func prepare(to statementHandle: inout OpaquePointer?, query: String) throws {
        if sqlite3_prepare_v2(databaseHandle, query, -1, &statementHandle, nil) != SQLITE_OK {
            let description = String(cString: sqlite3_errmsg(databaseHandle))
            
            Database.default.log.debug("🔶 ERROR: %@", description)
            throw DatabaseError.firstChance(.invalidQuery(query: query, description: description))
        }
    }
    
    @discardableResult
    private func step(to statementHandle: OpaquePointer?, result: inout CInt) -> Bool {
        return sqlite3_step(statementHandle) == SQLITE_ROW
    }
    
    private func bind(to statementHandle: OpaquePointer?, value: Value?, at index: CInt) throws {
        var result: CInt
        
        switch value {
        case let integer as Int:
            result = sqlite3_bind_int64(statementHandle, index, Int64(integer))
            
        case let string as String:
            result = sqlite3_bind_text(statementHandle, index, string, -1, DatabaseConnection.SQLITE_TRANSIENT)
            
        case let double as Double:
            result = sqlite3_bind_double(statementHandle, index, double)
            
        case let bool as Bool:
            result = sqlite3_bind_int(statementHandle, index, !bool ? 0 : 1)
            
        case let date as Date:
            result = sqlite3_bind_double(statementHandle, index, date.timeIntervalSinceReferenceDate)
            
        case let data as Data:
            result = data.withUnsafeBytes {
                sqlite3_bind_blob(statementHandle, index, $0.baseAddress, Int32($0.count), DatabaseConnection.SQLITE_TRANSIENT)
            }
            
        default:
            result = sqlite3_bind_null(statementHandle, index)
        }
        
        if result == SQLITE_ERROR {
            throw unknownError()
        }
    }
    
    private func unknownError(callStackSymbols: [String] = Thread.callStackSymbols) -> Error {
        let description = String(cString: sqlite3_errmsg(databaseHandle))
        
        Database.default.log.debug("🔶 ERROR: %@", description)
        return DatabaseError.firstChance(.unknown(description: description), callStackSymbols: callStackSymbols)
    }
}

extension DatabaseConnection: Hashable {
    public static func == (lhs: DatabaseConnection, rhs: DatabaseConnection) -> Bool {
        lhs.databaseHandle == rhs.databaseHandle
    }
    
    public func hash(into hasher: inout Hasher) {
        hasher.combine(databaseHandle.hashValue)
    }
}
