var app = app || {};

app.MetricSelector = Backbone.View.extend({

  tagName: 'button',
  className: 'btn btn-secondary btn-block metric-selector',
  attributes: {
    type: "button",
  },

  template: _.template('<%=label%>'),

  initialize: function(params) {
    this.label = params.label;
  },

  render: function() {
    this.$el.html(this.template({ label: this.label }));
  },

});
