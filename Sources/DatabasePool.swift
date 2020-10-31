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

public final class DatabasePool {
    public typealias Location = Database.Location
    public typealias KeyBlock = () -> String
    
    public static let `default` = DatabasePool()
    
    public var fileUrl: URL? {
        guard case let .file(fileName) = location else {
            return nil
        }
        
        return FileManager.default.inApplicationSupportDirectory(with: fileName)
    }
    
    public private(set) lazy var isPreparing = false
    
    private var location: Location?
    private var keyBlock: KeyBlock?
    
    private let lock = ReadWriteLock()
    private let queue = DispatchQueue(label: "com.database.queue", qos: .utility)
    private let log = Log(category: "Database Pool")
    
    private lazy var idles = Set<Database>()
    
    public func prepare(location: Location = .file(fileName: "Default.sqlite"), key keyBlock: @autoclosure @escaping KeyBlock, using block: (() -> Void)? = nil) {
        self.location = location
        self.keyBlock = keyBlock
        
        defer {
            isPreparing = false
        }
        
        isPreparing = true
        
        lock.write {
            block?()
        }
    }
    
    public func drain() {
        queue.sync {
            idles.removeAll(keepingCapacity: false)
        }
    }
    
    public func execute(_ query: Query) throws {
        delayIfNeeded()
        log.debug("Execute: %@", query.query)
        
        try perform { database in
            try database.execute(query)
        }
    }
    
    public func executeQuery(_ query: String) throws {
        delayIfNeeded()
        log.debug("Execute Query: %@", query)
        
        try perform { database in
            try database.exec(query: query)
        }
    }
    
    @inlinable
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> [T] {
        var items = [T]()
        try fetch(query, adaptee: adaptee) { items.append($0) }
        
        return items
    }
    
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T, using block: (T) -> Void) throws {
        delayIfNeeded()
        log.debug("Fetch: %@", query.query)
        
        try perform { database in
            try database.fetch(query, adaptee: adaptee, using: block)
        }
    }
    
    public func fetchOnce<T>(_ query: Query, adaptee: (_ statement: Statement) -> T?) throws -> T? {
        delayIfNeeded()
        log.debug("Fetch Once: %@", query.query)
        
        return try perform { try $0.scalar(query: query, adaptee: adaptee) }
    }
    
    public func migrateIfNeeded(creation: DatabaseMigrationProtocol.Type, migrations: DatabaseMigrationProtocol.Type...) throws {
        guard isPreparing else {
            preconditionFailure("Migrate only on prepare")
        }
        
        let migrations = migrations.sorted { $0.version > $1.version }
        
        if let migration = migrations.first, creation.version != migration.version {
            preconditionFailure("Invalid creation version")
        }
        
        let database = try Database(location: location.unsafelyUnwrapped, key: keyBlock.unsafelyUnwrapped())
        
        defer {
            idles.insert(database)
        }
        
        guard let version: Int = try? database.scalar(query: "PRAGMA user_version", adaptee: { $0[0] }), version != 0 else {
            try migrate(creation, in: database)
            return
        }
        
        for migration in migrations {
            guard version < migration.version else {
                continue
            }
            
            try migrate(migration, in: database)
        }
        
        log.debug("Database v%d", version)
    }
    
    private func migrate(_ migration: DatabaseMigrationProtocol.Type, in database: Database) throws {
        try database.exec(query: "BEGIN;\(migration.migrateQuery());COMMIT")
        try database.exec(query: "PRAGMA user_version=\(migration.version)")
    }
    
    private func perform<T>(action: (_ database: Database) throws -> T) rethrows -> T {
        return try lock.read {
            var database: Database!
            
            defer {
                _ = queue.sync {
                    idles.insert(database)
                }
            }
            
            queue.sync {
                if !idles.isEmpty {
                    database = idles.removeFirst()
                }
            }
            
            if database == nil {
                guard let location = location,
                      let keyBlock = keyBlock else {
                    preconditionFailure("Database not prepared")
                }
                
                database = try Database(location: location, key: keyBlock())
            }
            
            return try action(database)
        }
    }
    
    private func delayIfNeeded() {
        guard let delaySeconds = Query.delaySeconds else {
            return
        }
        
        Thread.sleep(forTimeInterval: delaySeconds)
    }
}

public extension DatabasePool {
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
