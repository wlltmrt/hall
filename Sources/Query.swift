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

public struct Query {
    public static var delaySeconds: TimeInterval? = nil
    
    @usableFromInline
    let query: String
    
    @usableFromInline
    let values: ContiguousArray<SQLiteValue?>?
    
    @inlinable
    public init(query: String, values: ContiguousArray<SQLiteValue?>?) {
        self.query = query
        self.values = values
    }
}

extension Query: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self.query = value
        self.values = nil
    }
}

extension Query: ExpressibleByStringInterpolation {
    public struct StringInterpolation: StringInterpolationProtocol {
        @usableFromInline
        var query: String
        
        @usableFromInline
        var values: ContiguousArray<SQLiteValue?>
        
        public init(literalCapacity: Int, interpolationCount: Int) {
            self.query = String(reserveCapacity: literalCapacity + interpolationCount)
            self.values = ContiguousArray<SQLiteValue?>(reserveCapacity: interpolationCount)
        }
        
        @inlinable
        public mutating func appendLiteral(_ literal: String) {
            query.append(literal)
        }
        
        @inlinable
        public mutating func appendInterpolation<T: SQLiteValue>(_ value: T?) {
            query.append("?")
            values.append(value)
        }
        
        @inlinable
        public mutating func appendInterpolation<T: RawRepresentable>(_ value: T?) where T.RawValue == Int {
            appendInterpolation(value?.rawValue)
        }
    }
    
    public init(stringInterpolation interpolation: StringInterpolation) {
        self.query = interpolation.query
        self.values = interpolation.values
    }
}
