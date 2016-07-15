var app = app || {};

app.Scope = Backbone.Model.extend({
  defaults: {
    metric: null,
  },
  initialize: function() {
    this.on("change:metric", function(self) {

    });
  }
});

app.scope = new app.Scope();
