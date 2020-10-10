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

public final class DatabasePool {
    public typealias Location = Database.Location
    public typealias KeyBlock = () -> String
    
    public static let `default` = DatabasePool()
    
    private var location: Location?
    private var keyBlock: KeyBlock?
    private var profiler: ProfilerProtocol?
    
    private let queue = DispatchQueue(label: "com.sqlite.pool", qos: .utility)
    private lazy var idles = Set<Database>()
    
    public func prepare(location: Location = .file(fileName: "Default.sqlite"), key keyBlock: @autoclosure @escaping KeyBlock, enableProfiler: Bool = false) throws {
        self.location = location
        self.keyBlock = keyBlock
        
        if enableProfiler {
            self.profiler = createProfilerIfSupported(category: "SQLite")
        }
    }
    
    public func drain() {
        queue.sync {
            idles.removeAll()
        }
    }
    
    public func execute(_ query: Query) throws {
        if let delaySeconds = Query.delaySeconds {
            Thread.sleep(forTimeInterval: delaySeconds)
        }
        
        let tracing = profiler?.begin(name: "Execute", query.query)
        
        defer {
            tracing?.end()
        }
        
        try perform { try $0.execute(query) }
    }
    
    public func executeQuery(_ query: String) throws {
        if let delaySeconds = Query.delaySeconds {
            Thread.sleep(forTimeInterval: delaySeconds)
        }
        
        let tracing = profiler?.begin(name: "Execute Query", query)
        
        defer {
            tracing?.end()
        }
        
        try perform { try $0.exec(query: query) }
    }
    
    @inlinable
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> [T] {
        var items = [T]()
        try fetch(query, adaptee: adaptee) { items.append($0) }
        
        return items
    }
    
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T, using block: (T) -> Void) throws {
        if let delaySeconds = Query.delaySeconds {
            Thread.sleep(forTimeInterval: delaySeconds)
        }
        
        let tracing = profiler?.begin(name: "Fetch", query.query)
        
        defer {
            tracing?.end()
        }
        
        try perform { try $0.fetch(query, adaptee: adaptee, using: block) }
    }
    
    public func fetchOnce<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> T? {
        if let delaySeconds = Query.delaySeconds {
            Thread.sleep(forTimeInterval: delaySeconds)
        }
        
        let tracing = profiler?.begin(name: "Fetch Once", query.query)
        
        defer {
            tracing?.end()
        }
        
        return try perform { try $0.scalar(query: query, adaptee: adaptee) }
    }
    
    private func perform<T>(action: (_ database: Database) throws -> T) throws -> T {
        let database: Database
        
        if !idles.isEmpty {
            database = queue.sync { idles.removeFirst() }
        }
        else {
            guard let location = location,
                  let keyBlock = keyBlock else {
                preconditionFailure("Pool is not prepared")
            }
            
            database = try Database(location: location, key: keyBlock())
        }
        
        defer {
            _ = queue.sync {
                idles.insert(database)
            }
        }
        
        return try action(database)
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
