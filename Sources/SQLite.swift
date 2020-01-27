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
import SQLite3
import Adrenaline

public enum SQLiteError: Error {
    case invalidQuery(query: String, description: String)
    case unknown(description: String)
}

public final class SQLite {
    public enum TransactionMode: String {
        case deferred = "DEFERRED"
        case exclusive = "EXCLUSIVE"
        case immediate = "IMMEDIATE"
    }
    
    private static let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    
    public static let `default` = SQLite()
    
    private var databaseHandle: OpaquePointer?
    private var profiler: Profiler?
    private let queue: DispatchQueue
    
    private init() {
        queue = DispatchQueue(label: "com.sqlite.queue", qos: .utility, attributes: .concurrent)
    }
    
    deinit {
        sqlite3_close(databaseHandle)
    }
    
    public func open(fileName: String = "default.db", enableProfiler: Bool = true) throws {
        try queue.sync {
            if enableProfiler {
                profiler = Profiler(category: "SQLite")
            }
            
            let fileManager = FileManager.default
            let path = fileManager.inDocumentDirectory(with: fileName).path
            
            if !fileManager.fileExists(atPath: path), let resourcePath = Bundle.main.resourcePath {
                try? fileManager.copyItem(atPath: URL(fileURLWithPath: resourcePath).appendingPathComponent(fileName).path, toPath: path)
            }
            
            if let databaseHandle = databaseHandle {
                sqlite3_close(databaseHandle)
            }
            
            if sqlite3_open_v2(path, &databaseHandle, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil) != SQLITE_OK {
                throw SQLiteError.unknown(description: "Can't open database: \(path)")
            }
            
            //profiler?.debug("%s opened", path)
            
            try execute("PRAGMA foreign_keys = ON")
        }
    }
    
    @discardableResult
    public func execute(_ query: Query) throws -> Int? {
        return try queue.sync {
            let tracing = profiler?.begin("%{public}s", query.query)
            
            defer {
                tracing?.end()
            }
            
            var statementHandle: OpaquePointer? = nil
            var result: CInt = 0
            
            if let values = query.values {
                if prepare(to: &statementHandle, query: query.query, result: &result) {
                    let bindCount = sqlite3_bind_parameter_count(statementHandle)
                    
                    for i in 1...bindCount {
                        try bind(to: statementHandle, value: values[Int(i) - 1], at: i)
                    }
                    
                    step(to: statementHandle, result: &result)
                    sqlite3_finalize(statementHandle)
                }
                else {
                    throw SQLiteError.invalidQuery(query: query.query, description: String(cString: sqlite3_errmsg(databaseHandle)))
                }
            }
            else {
                result = sqlite3_exec(databaseHandle, query.query, nil, nil, nil)
            }
            
            if result == SQLITE_ERROR {
                throw SQLiteError.unknown(description: String(cString: sqlite3_errmsg(databaseHandle)))
            }
            
            return executeResult()
        }
    }
    
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> [T] {
        return try queue.sync {
            let tracing = profiler?.begin("%{public}s", query.query)
            
            defer {
                tracing?.end()
            }
            
            var statementHandle: OpaquePointer? = nil
            var result: CInt = 0
            var elements = [T]()
            
            if prepare(to: &statementHandle, query: query.query, result: &result) {
                if let values = query.values {
                    let bindCount = sqlite3_bind_parameter_count(statementHandle)
                    
                    for i in 1...bindCount {
                        try bind(to: statementHandle, value: values[Int(i) - 1], at: i)
                    }
                }
                
                let statement = Statement(handle: statementHandle)
                
                while step(to: statementHandle, result: &result) {
                    elements.append(adaptee(statement))
                }
                
                sqlite3_finalize(statementHandle)
            }
            else {
                throw SQLiteError.invalidQuery(query: query.query, description: String(cString: sqlite3_errmsg(databaseHandle)))
            }
            
            if result == SQLITE_ERROR {
                throw SQLiteError.unknown(description: String(cString: sqlite3_errmsg(databaseHandle)))
            }
            
            return elements
        }
    }
    
    public func fetchOnce<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> T? {
        return try queue.sync {
            let tracing = profiler?.begin("%{public}s", query.query)
            
            defer {
                tracing?.end()
            }
            
            var statementHandle: OpaquePointer? = nil
            var result: CInt = 0
            var element: T?
            
            if prepare(to: &statementHandle, query: query.query, result: &result) {
                if let values = query.values {
                    let bindCount = sqlite3_bind_parameter_count(statementHandle)
                    
                    for i in 1...bindCount {
                        try bind(to: statementHandle, value: values[Int(i) - 1], at: i)
                    }
                }
                
                if step(to: statementHandle, result: &result) {
                    element = adaptee(Statement(handle: statementHandle))
                }
                
                sqlite3_finalize(statementHandle)
            }
            else {
                throw SQLiteError.invalidQuery(query: query.query, description: String(cString: sqlite3_errmsg(databaseHandle)))
            }
            
            if result == SQLITE_ERROR {
                throw SQLiteError.unknown(description: String(cString: sqlite3_errmsg(databaseHandle)))
            }
            
            return element
        }
    }
    
    public func transaction(_ mode: TransactionMode = .deferred, work: (_ sqlite: SQLite) throws -> Void) throws {
        try execute("BEGIN \(mode.rawValue) TRANSACTION")
        
        do {
            try work(self)
            try execute("COMMIT TRANSACTION")
        }
        catch {
            try execute("ROLLBACK TRANSACTION")
            throw error
        }
    }
    
    private func prepare(to statementHandle: inout OpaquePointer?, query: String, result: inout CInt) -> Bool {
        result = sqlite3_prepare_v2(databaseHandle, query, -1, &statementHandle, nil)
        return result == SQLITE_OK
    }
    
    @discardableResult
    private func step(to statementHandle: OpaquePointer?, result: inout CInt) -> Bool {
        return sqlite3_step(statementHandle) == SQLITE_ROW
    }
    
    private func bind(to statementHandle: OpaquePointer?, value: SQLiteValue?, at index: CInt) throws {
        var result: CInt
        
        switch value {
        case let bool as Bool:
            result = bool ? sqlite3_bind_int(statementHandle, index, 1) : sqlite3_bind_null(statementHandle, index)
            
        case let character as Character:
            result = sqlite3_bind_int(statementHandle, index, CInt(UnicodeScalar(String(character))!.value))
            
        case let data as Data:
            result = data.withUnsafeBytes {
                sqlite3_bind_blob(statementHandle, index, $0.baseAddress, Int32($0.count), SQLite.SQLITE_TRANSIENT)
            }
            
        case let date as Date:
            result = sqlite3_bind_int64(statementHandle, index, Int64(date.timeIntervalSinceReferenceDate))
            
        case let double as Double:
            result = sqlite3_bind_double(statementHandle, index, double)
            
        case let integer as Int:
            result = sqlite3_bind_int64(statementHandle, index, Int64(integer))
            
        case let string as String:
            result = sqlite3_bind_text(statementHandle, index, string, -1, SQLite.SQLITE_TRANSIENT)
            
        default:
            result = sqlite3_bind_null(statementHandle, index)
        }
        
        if result == SQLITE_ERROR {
            throw SQLiteError.unknown(description: String(cString: sqlite3_errmsg(databaseHandle)))
        }
    }
    
    private func execute(_ query: String) throws {
        try queue.sync {
            if sqlite3_exec(databaseHandle, query, nil, nil, nil) == SQLITE_ERROR {
                throw SQLiteError.unknown(description: String(cString: sqlite3_errmsg(databaseHandle)))
            }
        }
    }
    
    private func executeResult() -> Int? {
        var result = Int(sqlite3_last_insert_rowid(databaseHandle))
        
        if result == 0 {
            result = Int(sqlite3_changes(databaseHandle))
            profiler?.debug("%d changes", result)
        }
        else {
            sqlite3_set_last_insert_rowid(databaseHandle, 0)
            profiler?.debug("%d created", result)
        }
        
        return result > 0 ? result : nil
    }
}
