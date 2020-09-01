# Restricted HTTP Client
This library is wrapper to restrict the number of requests made by the Golang default HTTP Client. The main idea to develop this is to provide a way to preserve resources on servers that not has protections against a large number of connections .

## Documentation
As a wrapper, it has the same public functions from the GO's http client library:
https://golang.org/pkg/net/http/#Client

## Example:
```
c := &restrictedhttpclient.Configuration{
	MaxSimultaneousRequests:   10,
	RequestTimeout:            *funks.ForceNewStringDuration("5s"),
	SkipCertificateValidation: true,
}
client, err := restrictedhttpclient.New(c)
if err != nil {
    panic(err)
}
res, err := client.Get("https://www.uol.com.br")
...
```