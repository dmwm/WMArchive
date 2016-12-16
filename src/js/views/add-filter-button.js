// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.AddFilterButton = Backbone.View.extend({

  tagName: 'fieldset',
  className: 'filter-container form-group',

  template: _.template('<button type="button" class="btn btn-secondary">+</button>'),

  render: function() {
    this.$el.html(this.template());
    return this;
  },

});
