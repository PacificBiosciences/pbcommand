# CHANGELOG

## 1.7.0

- expose `SmrtLinkAuthClient` to `pbcommand.services`
- mark `ServiceAccessLayer` as deprecated. For SL >= 6.X.X, unauth'ed access is limited to internal usecases, such as `pbtestkit-service-runner` and `pbsmrtpipe`. 