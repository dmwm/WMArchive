var app = app || {};

app.TimeframeSelector = Backbone.View.extend({

  template: _.template('<input type="date" class="form-control" id="timeframe-selector">'),

  render: function() {
    this.$el.html(this.template());
    $('#timeframe-selector').daterangepicker();
  },

});
