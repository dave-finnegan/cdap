angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetExploreController',
    function($scope, MyDataSource, QueryModel, $state, myHelpers, $log) {


      var dataSrc = new MyDataSource($scope);
      var dataModel = new QueryModel(dataSrc, 'exploreQueries');

      $scope.activePanel = 0;

      dataSrc
        .request({
          _cdapNsPath: '/data/explore/tables'
        })
        .then(function (result) {
          $scope.tables = result;
        });

    }
  );
