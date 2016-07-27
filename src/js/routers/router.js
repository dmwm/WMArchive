var app = app || {};

app.Router = Backbone.Router.extend({

  routes: {
    '' : 'home',
    'performance': 'performance',
    'apis': 'apis',
  },

  execute: function(callback, args, name) {
    args.push(parseQueryString(args.pop()));
    if (callback) callback.apply(this, args);
  },

  home: function() {
    Backbone.history.navigate('performance', { trigger: true });
  },
  performance: function(query) {
    this.showPage('performance');
    app.scope.setQuery(query);
  },
  apis: function() {
    this.showPage('apis');
  },

  showPage: function(page) {
    if (page == null) {
      page = 'home';
    }
    switch (page) {
      case 'performance':
        var view = new app.PerformanceView({ el: $('#main')});
        break;
      default:
        var view = new app.InfoView({ el: $('#main'), templateFile: page });
        break;
    }
    view.render();
  },

});

function parseQueryString(queryString) {
  var params = {};
    if(queryString){
      _.each(
        _.map(decodeURI(queryString).split(/&/g),function(el,i){
          var aux = el.split('='), o = {};
          if(aux.length >= 1){
            var val = undefined;
            if(aux.length == 2)
              val = aux[1];
            o[aux[0]] = val;
          }
          return o;
        }),
        function(o){
          console.log(o);
          for (var key of Object.keys(o)) {
            if (key.endsWith('[]')) {
              var new_key = key.substring(0, key.length-2);
              console.log(new_key);
              var existing = params[new_key] || [];
              existing.push(o[key]);
              delete o[key];
              o[new_key] = existing;
            }
          }
          console.log(o);
          _.extend(params,o);
        }
      );
    }
    return params;
}
