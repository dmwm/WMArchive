var app = app || {};

app.Router = Backbone.Router.extend({

  routes: {
    '' : 'home',
    'performance': 'performance',
    'apis': 'apis',
  },

  home: function() {
    Backbone.history.navigate('performance', { trigger: true });
  },
  performance: function() {
    this.showPage('performance');
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
