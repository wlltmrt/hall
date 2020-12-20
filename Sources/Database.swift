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
import ConcurrentKit

public final class Database {
    public typealias Location = DatabaseConnection.Location
    public typealias KeyBlock = () -> String
    
    public static let `default` = Database()
    
    public var fileUrl: URL? {
        guard case let .file(fileName) = location else {
            return nil
        }
        
        return FileManager.default.inApplicationSupportDirectory(with: fileName)
    }
    
    private var location: Location?
    private var keyBlock: KeyBlock?
    
    private let lock = ReadWriteLock()
    private let log = Log(category: "Database")
    private let queue = DispatchQueue(label: "com.database.queue", qos: .userInitiated)
    
    private lazy var idles = Set<DatabaseConnection>()
    
    #if DEBUG
    private lazy var isPreparing = false
    #endif
    
    private init() {
    }
    
    public func prepare(location: Location = .file(fileName: "Default.sqlite"), key keyBlock: @autoclosure @escaping KeyBlock, using block: ((_ database: Database) -> Void)? = nil) {
        self.location = location
        self.keyBlock = keyBlock
        
        DispatchQueue.utility.async { [self] in
            lock.write {
                #if DEBUG
                isPreparing = true
                #endif
                
                block?(self)
                
                #if DEBUG
                isPreparing = false
                #endif
            }
        }
        
        usleep(250)
    }
    
    public func drain() {
        log?.debug("Drain")
        
        queue.sync {
            idles.removeAll(keepingCapacity: false)
        }
    }
    
    public func execute(_ query: Query) throws {
        try perform {
            delayIfNeeded()
            log?.debug("Execute: %@", query.query)
            
            try $0.execute(query)
        }
    }
    
    public func executeQuery(_ query: String) throws {
        try perform {
            delayIfNeeded()
            log?.debug("Execute query: %@", query)
            
            try $0.exec(query: query)
        }
    }
    
    @inlinable
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> [T] {
        var items = [T]()
        
        try fetch(query, adaptee: adaptee) {
            items.append($0)
        }
        
        return items
    }
    
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T, using block: (T) -> Void) throws {
        try perform {
            delayIfNeeded()
            log?.debug("Fetch: %@", query.query)
            
            try $0.fetch(query, adaptee: adaptee, using: block)
        }
    }
    
    public func fetchOnce<T>(_ query: Query, adaptee: (_ statement: Statement) -> T?) throws -> T? {
        return try perform {
            delayIfNeeded()
            log?.debug("Fetch once: %@", query.query)
            
            return try $0.scalar(query: query, adaptee: adaptee)
        }
    }
    
    public func createOrMigrateIfNeeded(schema: DatabaseSchemaProtocol.Type, willMigrate: (() -> Void)? = nil) throws {
        #if DEBUG
        guard isPreparing else {
            preconditionFailure("Only works on prepare")
        }
        #endif
        
        try performUnsafe {
            try createOrMigrateIfNeeded(connection: $0, schema: schema, willMigrate: willMigrate)
            try existsTables(schema, in: $0)
        }
    }
    
    public func recreate(schema: DatabaseSchemaProtocol.Type) throws {
        #if DEBUG
        guard isPreparing else {
            preconditionFailure("Only works on prepare")
        }
        #endif
        
        try performUnsafe {
            if let fileUrl = fileUrl {
                try FileManager.default.removeItem(at: fileUrl)
            }
            
            try migrate(schema, in: $0)
            try existsTables(schema, in: $0)
        }
    }
    
    private func createOrMigrateIfNeeded(connection: DatabaseConnection, schema: DatabaseSchemaProtocol.Type, willMigrate: (() -> Void)? = nil) throws {
        guard let version: Int = try? connection.scalar(query: "PRAGMA user_version", adaptee: { $0[0] }), version != 0 else {
            try migrate(schema, in: connection)
            return
        }
        
        var migrations = schema.migrations.sorted { $0.version < $1.version }
        
        if let migration = migrations.last, schema.version != migration.version {
            preconditionFailure("Invalid schema version")
        }
        
        if schema.version < version {
            let description = "Database v\(version) not supported"
            
            log?.debug("🔶 ERROR: %@", description)
            throw DatabaseError.firstChance(.verificationFailed(description: description))
        }
        
        migrations = migrations.filter { $0.version > version }
        
        guard !migrations.isEmpty else {
            log?.debug("Database v%d", version)
            return
        }
        
        willMigrate?()
        
        for migration in migrations {
            try migrate(migration, in: connection)
        }
    }
    
    private func delayIfNeeded() {
        guard let delaySeconds = Query.delaySeconds else {
            return
        }
        
        Thread.sleep(forTimeInterval: delaySeconds)
    }
    
    private func existsTables(_ schema: DatabaseSchemaProtocol.Type, in connection: DatabaseConnection) throws {
        var tables = schema.tables
        
        try fetch("SELECT name FROM sqlite_master WHERE type='table'", adaptee: { $0[0] }) {
            tables.remove($0)
        }
        
        guard !tables.isEmpty else {
            return
        }
        
        let description = "No table: \(tables.joined(separator: ", "))"
        
        log?.debug("🔶 ERROR: %@", description)
        throw DatabaseError.firstChance(.verificationFailed(description: description))
    }
    
    private func migrate(_ migration: DatabaseMigrationProtocol.Type, in connection: DatabaseConnection) throws {
        let version = migration.version
        
        try connection.exec(query: "BEGIN;\(migration.migrateQuery());COMMIT")
        try connection.exec(query: "PRAGMA user_version=\(version)")
        
        log?.debug("Migration v%d success", version)
    }
    
    private func perform<T>(action: (_ connection: DatabaseConnection) throws -> T) rethrows -> T {
        return try lock.read {
            var connection: DatabaseConnection!
            
            defer {
                _ = queue.sync {
                    idles.insert(connection)
                }
            }
            
            queue.sync {
                if !idles.isEmpty {
                    connection = idles.removeFirst()
                }
            }
            
            if connection == nil {
                guard let location = location,
                      let keyBlock = keyBlock else {
                    preconditionFailure("Database not prepared")
                }
                
                connection = try DatabaseConnection(location: location, keyBlock: keyBlock, log: log)
            }
            
            return try action(connection)
        }
    }
    
    private func performUnsafe<T>(action: (_ connection: DatabaseConnection) throws -> T) throws -> T {
        var connection: DatabaseConnection!
        
        defer {
            idles.insert(connection)
        }
        
        if !idles.isEmpty {
            connection = idles.removeFirst()
        }
        
        if connection == nil {
            guard let location = location,
                  let keyBlock = keyBlock else {
                preconditionFailure("Database not prepared")
            }
            
            connection = try DatabaseConnection(location: location, keyBlock: keyBlock, log: log)
        }
        
        return try action(connection)
    }
}

public extension Database {
    @inlinable
    func fetchValue(_ query: Query, defaultValue defaultBlock: @autoclosure () -> Bool) throws -> Bool {
        return try fetchOnce(query) { $0[0] } ?? defaultBlock()
    }
    
    @inlinable
    func fetchValue(_ query: Query, defaultValue defaultBlock: @autoclosure () -> Data) throws -> Data {
        return try fetchOnce(query) { $0[0] } ?? defaultBlock()
    }
    
    @inlinable
    func fetchValue(_ query: Query, defaultValue defaultBlock: @autoclosure () -> Date) throws -> Date {
        return try fetchOnce(query) { $0[0] } ?? defaultBlock()
    }
    
    @inlinable
    func fetchValue(_ query: Query, defaultValue defaultBlock: @autoclosure () -> Double) throws -> Double {
        return try fetchOnce(query) { $0[0] } ?? defaultBlock()
    }
    
    @inlinable
    func fetchValue(_ query: Query, defaultValue defaultBlock: @autoclosure () -> Int) throws -> Int {
        return try fetchOnce(query) { $0[0] } ?? defaultBlock()
    }
    
    @inlinable
    func fetchValue(_ query: Query, defaultValue defaultBlock: @autoclosure () -> String) throws -> String {
        return try fetchOnce(query) { $0[0] } ?? defaultBlock()
    }
}
