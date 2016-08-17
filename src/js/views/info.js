var app = app || {};

app.InfoView = Backbone.View.extend({

  initialize: function(params) {
    this.templateFile = params.templateFile;
  },

  render: function() {
    var el = this.$el;
    var templateFile = this.templateFile;
    $.get('/wmarchive/web/static/templates/' + templateFile + '.html', function (data) {
      template = _.template('<div class="container" id="info"><div class="row"><div class="col-sm-12">' + data + '</div></div></div>');
      el.html(template);
    }, 'html');
  }

});
