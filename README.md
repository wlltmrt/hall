# Hall

[![Build Status](https://travis-ci.org/wellmart/hall.svg?branch=master)](https://travis-ci.org/wellmart/hall)
[![Swift 5](https://img.shields.io/badge/swift-5-blue.svg)](https://developer.apple.com/swift/)
![Version](https://img.shields.io/badge/version-0.1.0-blue)
[![Software License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)
[![Swift Package Manager compatible](https://img.shields.io/badge/swift%20package%20manager-compatible-blue.svg)](https://github.com/apple/swift-package-manager)

The Hall provides an abstraction layer over SQLite to allow database access.

## Requirements

Swift 5 and beyond.

## Usage

```swift
import Hall

struct User {
    var name: String
}

func main() {
    do {
        try SQLite.default.open()
        try SQLite.default.execute("CREATE TABLE users...")
            
        let results = try SQLite.default.fetch("SELECT name,... FROM users") { statement in
            return User(name: statement[0])
        }
            
        if let results = results {
            print(results)
        }
    } catch {
        print(error)
    }
}
```

## License

[MIT](https://choosealicense.com/licenses/mit/)
