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
import SQLCipher

public struct Statement {
    @usableFromInline
    let handle: OpaquePointer?
    
    @inlinable
    init(handle: OpaquePointer?) {
        self.handle = handle
    }
    
    @inlinable
    public subscript(index: CInt) -> Bool {
        return sqlite3_column_int(handle, index) != 0
    }
    
    public subscript(index: CInt) -> Bool? {
        return isNull(index) ? nil : sqlite3_column_int(handle, index) != 0
    }
    
    @inlinable
    public subscript(index: CInt) -> Data {
        return Data(bytes: sqlite3_column_blob(handle, index), count: Int(sqlite3_column_bytes(handle, index)))
    }
    
    @inlinable
    public subscript(index: CInt) -> Data? {
        return isNull(index) ? nil : Data(bytes: sqlite3_column_blob(handle, index), count: Int(sqlite3_column_bytes(handle, index)))
    }
    
    @inlinable
    public subscript(index: CInt) -> Date {
        return Date(timeIntervalSinceReferenceDate: sqlite3_column_double(handle, index))
    }
    
    @inlinable
    public subscript(index: CInt) -> Date? {
        return isNull(index) ? nil : Date(timeIntervalSinceReferenceDate: sqlite3_column_double(handle, index))
    }
    
    @inlinable
    public subscript(index: CInt) -> Double {
        return sqlite3_column_double(handle, index)
    }
    
    @inlinable
    public subscript(index: CInt) -> Double? {
        return isNull(index) ? nil : sqlite3_column_double(handle, index)
    }
    
    @inlinable
    public subscript(index: CInt) -> Int {
        return Int(sqlite3_column_int64(handle, index))
    }
    
    @inlinable
    public subscript(index: CInt) -> Int? {
        return isNull(index) ? nil : Int(sqlite3_column_int64(handle, index))
    }
    
    @inlinable
    public subscript(index: CInt) -> String {
        return String(cString: sqlite3_column_text(handle, index))
    }
    
    @inlinable
    public subscript(index: CInt) -> String? {
        return isNull(index) ? nil : String(cString: sqlite3_column_text(handle, index))
    }
    
    @inlinable
    public subscript<T: RawRepresentable>(index: CInt) -> T where T.RawValue == Int {
        return T(rawValue: Int(sqlite3_column_int64(handle, index))).unsafelyUnwrapped
    }
    
    @inlinable
    public subscript<T: RawRepresentable>(index: CInt) -> T? where T.RawValue == Int {
        return T(rawValue: Int(sqlite3_column_int64(handle, index)))
    }
    
    @inlinable
    func isNull(_ index: CInt) -> Bool {
        return sqlite3_column_type(handle, index) == SQLITE_NULL
    }
}
