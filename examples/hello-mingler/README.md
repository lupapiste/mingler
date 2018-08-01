# hello-mingler

Sample application for [Mingler](https://github.com/lupapiste/mingler) library.

# Run

With repl:

```bash
$ lein repl
user=>  (require '[hello-mingler.core :as hello])
nil
user=> (hello/-main "Greetings")
{:_id #inst "2018-08-01T09:05:12.704-00:00", :args ["Greetings"]}
nil
user=> (hello/-main "Mongler" "says" "hi")
{:_id #inst "2018-08-01T09:05:30.606-00:00", :args ["Mongler" "says" "hi"]}
{:_id #inst "2018-08-01T09:05:12.704-00:00", :args ["Greetings"]}
nil
user=> 
```

Or with überjar:

```bash
$ lein uberjar
$ java -jar target/hello-mingler.jar Greetings
{:_id #inst "2018-08-01T07:57:10.854-00:00", :args ["Greetings"]}
$ java -jar target/hello-mingler.jar Mingler says hi to MongoDB
{:_id #inst "2018-08-01T07:57:20.117-00:00", :args ["Mingler" "says" "hi" "to" "MongoDB"]}
{:_id #inst "2018-08-01T07:57:10.854-00:00", :args ["Greetings"]}
```

## License

Copyright © 2018 [Evolta Ltd](http://evolta.fi)

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
