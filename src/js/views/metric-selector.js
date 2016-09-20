// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.MetricSelector = Backbone.View.extend({

  tagName: 'button',
  className: 'btn btn-secondary btn-block metric-selector',
  attributes: {
    type: "button",
  },

  template: _.template('<%=label%>'),

  initialize: function(options) {
    _.extend(this, _.pick(options, 'label', 'name', 'description'));
  },

  render: function() {
    this.$el.html(this.template({ label: this.label }));
    this.$el.attr('name', this.name.replace(".", "__"));
    this.$el.attr('data-toggle', 'tooltip');
    this.$el.attr('title', this.description);
    this.$el.tooltip();
    return this;
  },

});
