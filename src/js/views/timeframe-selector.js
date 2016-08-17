var app = app || {};

app.TimeframeSelector = Backbone.View.extend({

  tagName: 'fieldset',
  className: 'filter-container form-group',
  template: _.template('<div class="form-group"><label class="form-control-label" for="timeframe-selector">TIMEFRAME</label><div class="filter-text-container"><input type="date" class="form-control filter-text" id="timeframe-selector"></div></div>'),

  render: function() {
    this.$el.html(this.template());
    this.model = app.scope;
    this.renderPicker();
    return this;
  },

  renderPicker: function() {
    var self = this;
    $('#timeframe-selector').daterangepicker({
        locale: {
          format: app.format_time_daterangepicker,
        },
        startDate: self.model.get('start_date'),
        endDate: self.model.get('end_date'),
      },
      function(start, end, label) {
        self.model.set({ start_date: start, end_date: end });
      }
    );
    return this;
  }

});
