var app = app || {};

app.TimeframeSelector = Backbone.View.extend({

  template: _.template('<input type="date" class="form-control" id="timeframe-selector">'),

  render: function() {
    this.$el.html(this.template());
    this.model = app.scope;
    var self = this;
    $('#timeframe-selector').daterangepicker({
        locale: {
          format: 'DD/MM/YYYY',
        },
        startDate: self.model.get('start_date'),
        endDate: self.model.get('end_date'),
      },
      function(start, end, label) {
        self.model.set({ start_date: start, end_date: end });
      }
    );
  },

});
