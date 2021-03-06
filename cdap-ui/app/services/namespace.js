angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespace($q, MyDataSource) {

    this.namespaceList = [];

    var data = new MyDataSource(),
        queryInProgress = null;


    this.getList = function (force) {
      if (!force && this.namespaceList.length) {
          return $q.when(this.namespaceList);
      }

      if (!queryInProgress) {

        queryInProgress = $q.defer();

        data.request(
          {
            _cdapPath: '/namespaces',
            method: 'GET'
          },
          (function(res) {

            if(!res.length) {
              res = [{
                id: 'default',
                name: 'default'
              }];
            }

            this.namespaceList = res;
            queryInProgress.resolve(res);
            queryInProgress = null;
          }).bind(this)
        );

      }

      return queryInProgress.promise;
    };

    this.getDisplayName = function(name) {
      var ns = this.namespaceList.filter(function(namespace) {
        return namespace.name === name;
      });
      return ns[0].name || id;
    };

  });
