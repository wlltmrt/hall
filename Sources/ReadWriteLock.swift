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

final class ReadWriteLock {
    @usableFromInline
    var lock = pthread_rwlock_t()
    
    @inlinable
    init() {
        pthread_rwlock_init(&lock, nil)
    }
    
    @inlinable
    func read<T>(execute work: () throws -> T) rethrows -> T {
        defer {
            pthread_rwlock_unlock(&lock)
        }
        
        pthread_rwlock_rdlock(&lock)
        return try work()
    }
    
    @inlinable
    func write(execute work: () throws -> Void) rethrows {
        defer {
            pthread_rwlock_unlock(&lock)
        }
        
        pthread_rwlock_wrlock(&lock)
        try work()
    }
    
    deinit {
        pthread_rwlock_destroy(&lock)
    }
}
