angular.module(PKG.name+'.services')
  /*
    MyDataSource // usage in a controler:

    var dataSrc = new MyDataSource($scope);

    // polling a namespaced resource example:
    dataSrc.poll({
        method: 'GET',
        _cdapNsPath: '/foo/bar'
      },
      function(result) {
        $scope.foo = result;
      }
    ); // will poll <host>:<port>/v3/namespaces/<currentNamespace>/foo/bar

    // posting to a systemwide resource:
    dataSrc.request({
        method: 'POST',
        _cdapPath: '/system/config',
        body: {
          foo: 'bar'
        }
      },
      function(result) {
        $scope.foo = result;
      }
    ); // will post to <host>:<port>/v3/system/config
   */
  .factory('MyDataSource', function ($log, $rootScope, caskWindowManager, mySocket,
    MYSOCKET_EVENT, $q, MyPromise, $timeout, myCdapUrl) {

    var instances = {}; // keyed by scopeid

    function _pollStart (resource) {
      var re = {};

      if (!resource.url) {
        re = resource;
      } else {
        re = {
          url: resource.url,
          json: true,
          method: resource.method
        };
      }

      mySocket.send({
        action: 'poll-start',
        resource: re
      });
    }

    function _pollStop (resource) {

      var re = {};
      if (!resource.url) {
        re = resource;
      }else {
        re = {
          url: resource.url,
          json: true,
          method: resource.method
        };
      }
      mySocket.send({
        action: 'poll-stop',
        resource: re
      });
    }

    $rootScope.$on(MYSOCKET_EVENT.reconnected, function () {
      // $log.log('[DataSource] reconnected, reloading...');

      // https://github.com/angular-ui/ui-router/issues/582
      // $state.transitionTo($state.current, $state.$current.params,
      //   { reload: true, inherit: true, notify: true }
      // );
      window.$go('home');
    });

    function DataSource (scope) {
      scope = scope || $rootScope.$new();
      var id = scope.$id,
          self = this;

      if(instances[id]) {
        // throw new Error('multiple DataSource for scope', id);
        return instances[id];
      }

      if (!(this instanceof DataSource)) {
        return new DataSource(scope);
      }

      instances[id] = self;

      this.bindings = [];

      scope.$on(MYSOCKET_EVENT.message, function (event, data) {
        var match = data.resource;

        if(data.statusCode>299 || data.warning) {
          angular.forEach(self.bindings, function (b) {
            if(angular.equals(b.resource, match)) {
              if (b.errorCallback) {
                $rootScope.$applyAsync(b.errorCallback.bind(null, data.response));
              } else if (b.reject) {
                $rootScope.$applyAsync(b.reject.bind(null, {data: data.response}));
              }
            }
          });
          return; // errors are handled at $rootScope level
        }
        // Not using angular.forEach for performance reasons.
        for (var i=0; i<self.bindings.length; i++) {
          var b = self.bindings[i];
          if(angular.equals(b.resource, match)) {
            if (angular.isFunction(b.callback)) {
              scope.$apply(b.callback.bind(null, data.response));
            } else if (b && b.resolve) {
              // https://github.com/angular/angular.js/wiki/When-to-use-$scope.$apply%28%29
              scope.$apply(b.resolve.bind(null, {data: data.response}));
              return;
            }
          }
        }
      });

      scope.$on('$destroy', function () {
        delete instances[id];
      });

      scope.$on(caskWindowManager.event.blur, function () {
        angular.forEach(self.bindings, function (b) {
          if(b.poll) {
            _pollStop(b.resource);
          }
        });
      });

      scope.$on(caskWindowManager.event.focus, function () {
        angular.forEach(self.bindings, function (b) {
          if(b.poll) {
            _pollStart(b.resource);
          }
        });
      });

      this.scope = scope;
    }



    /**
     * poll a resource
     */
    DataSource.prototype.poll = function (resource, cb) {
      var self = this;
      var prom = new MyPromise(function(resolve, reject) {
        var generatedResource = {
          json: true,
          method: resource.method || 'GET'
        };
        if (!resource.url) {
          generatedResource.url = buildUrl(myCdapUrl.constructUrl(resource), resource.params || {});
        } else {
          generatedResource.url = buildUrl(resource.url, resource.params || {});
        }

        self.bindings.push({
          poll: true,
          resource: generatedResource,
          callback: cb,
          resolve: resolve,
          reject: reject
        });
        self.scope.$on('$destroy', function () {
          _pollStop(generatedResource);
        });

        _pollStart(generatedResource);
      }, true);
      return prom;
    };

    DataSource.prototype.pollStop = function(resource) {
      var prom = new MyPromise(function(resolve, reject) {
        _pollStop(resource);
      });
      return prom;
    };


    /**
     * fetch a resource
     */
    DataSource.prototype.request = function (resource, cb) {
      var self = this;
      var prom = new MyPromise(function(resolve, reject) {

        var generatedResource = {
          json: true,
          method: resource.method
        };
        if (!resource.url) {
          generatedResource.url = buildUrl(myCdapUrl.constructUrl(resource), resource.params || {});
        } else {
          generatedResource.url = buildUrl(resource.url, resource.params || {});
          if (resource.data) {
            angular.extend(generatedResource, resource.data);
          }
        }

        self.bindings.push({
          resource: generatedResource,
          callback: cb,
          resolve: resolve,
          reject: reject
        });

        mySocket.send({
          action: 'request',
          resource: generatedResource
        });

      }, false);
      if (!resource.$isResource) {
        prom = prom.then(function(res) {
          res = res.data;
          return res;
        });
      }
      return prom;
    };

    return DataSource;
  });

function buildUrl(url, params) {
  if (!params) return url;
  var parts = [];

  function forEachSorted(obj, iterator, context) {
    var keys = Object.keys(params).sort();
    for (var i = 0; i < keys.length; i++) {
      iterator.call(context, obj[keys[i]], keys[i]);
    }
  return keys;
  }

  function encodeUriQuery(val, pctEncodeSpaces) {
    return encodeURIComponent(val).
           replace(/%40/gi, '@').
           replace(/%3A/gi, ':').
           replace(/%24/g, '$').
           replace(/%2C/gi, ',').
           replace(/%3B/gi, ';').
           replace(/%20/g, (pctEncodeSpaces ? '%20' : '+'));
  }

  forEachSorted(params, function(value, key) {
    if (value === null || angular.isUndefined(value)) return;
    if (!angular.isArray(value)) value = [value];

    angular.forEach(value, function(v) {
      if (angular.isObject(v)) {
        if (angular.isDate(v)) {
          v = v.toISOString();
        } else {
          v = toJson(v);
        }
      }
      parts.push(encodeUriQuery(key) + '=' +
                 encodeUriQuery(v));
    });
  });
  if (parts.length > 0) {
    url += ((url.indexOf('?') == -1) ? '?' : '&') + parts.join('&');
  }
  return url;
}
