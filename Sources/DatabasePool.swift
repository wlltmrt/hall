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
    
    private lazy var actives = Set<Database>()
    private lazy var inactives = Set<Database>()
    
    private var location: Location?
    private var keyBlock: KeyBlock?
    
    private let queue = DispatchQueue(label: "com.sqlite.pool", qos: .utility, attributes: .concurrent)
    
    public func prepare(location: Location = .file(fileName: "Default.sqlite"), key keyBlock: @autoclosure @escaping KeyBlock) throws {
        self.location = location
        self.keyBlock = keyBlock
    }
    
    public func execute(_ query: Query) throws {
        try perform { try $0.execute(query) }
    }
    
    public func executeQuery(_ query: String) throws {
        try perform { try $0.executeQuery(query) }
    }
    
    @inlinable
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> [T] {
        var items = [T]()
        try fetch(query, adaptee: adaptee) { items.append($0) }
        
        return items
    }
    
    public func fetch<T>(_ query: Query, adaptee: (_ statement: Statement) -> T, using block: (T) -> Void) throws {
        try perform { try $0.fetch(query, adaptee: adaptee, using: block) }
    }
    
    public func fetchOnce<T>(_ query: Query, adaptee: (_ statement: Statement) -> T) throws -> T? {
        return try perform { try $0.fetchOnce(query, adaptee: adaptee) }
    }
    
    private func perform<T>(action: (_ database: Database) throws -> T) throws -> T {
        let database: Database
        
        if !inactives.isEmpty {
            database = inactives.removeFirst()
        } else {
            guard let location = location,
                  let keyBlock = keyBlock else {
                preconditionFailure("")
            }
            
            database = try Database(location: location, key: keyBlock())
        }
        
        actives.insert(database)
        
        defer {
            actives.remove(database)
            inactives.insert(database)
        }
        
        return try queue.sync { try action(database) }
    }
}
