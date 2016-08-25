var app = app || {};

app.TimeframeSelector = Backbone.View.extend({

  tagName: 'fieldset',
  className: 'filter-container form-group filter-active',
  template: _.template('<div class="form-group"><label class="form-control-label" for="timeframe-selector">TIMEFRAME</label><div class="filter-text-container"><input type="text" class="form-control filter-text" id="timeframe-selector"></div></div>'),

  initialize: function() {
    var self = this;
    this.model = app.scope;
    this.listenTo(this.model, 'change:status', this.renderPicker);
    this.listenTo(this.model, 'change:start_date change:end_date', function() {
      var picker = $('#timeframe-selector').data('daterangepicker');
      picker.setStartDate(self.model.get('start_date'));
      picker.setEndDate(self.model.get('end_date'));
    });
  },

  render: function() {
    this.$el.html(this.template());
    this.renderPicker();
    return this;
  },

  renderPicker: function() {
    var self = this;
    $('#timeframe-selector').daterangepicker({
        locale: {
          format: app.format_time_daterangepicker,
          // separator: ' to ',
          customRangeLabel: "Custom Timeframe",
        },
        minDate: moment((self.model.get('status') || {}).min_date),
        maxDate: moment((self.model.get('status') || {}).max_date),
        startDate: self.model.get('start_date'),
        endDate: self.model.get('end_date'),
        ranges: {
           'Today': [moment(), moment()],
           'Yesterday': [moment().subtract(1, 'days'), moment().subtract(1, 'days')],
           'Last 7 Days': [moment().subtract(6, 'days'), moment()],
           'Last 30 Days': [moment().subtract(29, 'days'), moment()],
           'This Month': [moment().startOf('month'), moment().endOf('month')],
           'Last Month': [moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month')]
        },
        opens: 'left',
        alwaysShowCalendars: true,
        autoApply: true,
      },
      function(start, end, label) {
        self.model.set({ start_date: start, end_date: end });
      }
    );
    return this;
  }

});
