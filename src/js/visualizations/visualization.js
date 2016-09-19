// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.VisualizationView = Backbone.View.extend({

  title: '',

  limit: 5,
  page: 0,
  showAll: false,

  initialize: function(options) {
    _.extend(this, _.pick(options, 'data', 'metric', 'axis', 'supplementaryData'));
  },

  render: function() {
    this.$el.empty();

    var canvas = d3.select(this.el);

    var axis_title = app.scope.filters[this.axis];

    var map_value = function(item) {
      if (item.jobstates != null) {
        return d3.sum(item.jobstates.map(function(d) { return d.count }));
      } else {
        return item.average;
      }
    }
    var display_data = this.data.slice(0)
      .sort(function(lhs, rhs) { return map_value(rhs) - map_value(lhs)  })


    if (this.data.length <= this.limit) {

      this.renderVisualization(canvas, display_data);

    } else {

      // Top
      var max_container = canvas.append('section');
      var title_container = max_container.append('h6').attr('class', 'canvas-title').text("Top " + this.limit + " " + axis_title + "s");
      var paging_container = title_container.append('small').attr('class', 'paging text-muted pull-xs-right');
      var page_count = Math.ceil(this.data.length / this.limit);
      if (this.page > 1) {
        paging_container.append('a').attr('class', 'paging-button glyphicon-step-backward').attr('data-page-target', 0);
      }
      if (this.page > 0) {
        paging_container.append('a').attr('class', 'paging-button glyphicon-chevron-left').attr('data-page-target', this.page - 1);
      }
      paging_container.append('span').attr('class', 'paging-text').text((this.page + 1) + ' / ' + page_count);
      if (this.page < page_count - 1) {
        paging_container.append('a').attr('class', 'paging-button glyphicon-chevron-right').attr('data-page-target', this.page + 1);
      }
      if (this.page < page_count - 2) {
        paging_container.append('a').attr('class', 'paging-button glyphicon-step-forward').attr('data-page-target', page_count - 1);
      }
      var max_canvas = max_container.append('div')
      max_container.append('small').attr('class', 'text-muted')
        .append('a').text('Show more...').attr('class', 'increase-limit-button').attr('data-limit-target', this.limit + 5);

      this.renderVisualization(max_canvas, display_data);

      // All
      var all_container = canvas.append('section');
      var title_container = all_container.append('h6').attr('class', 'canvas-title').text("All " + this.data.length + " " + axis_title + "s");
      var display_button = title_container.append('small').attr('class', 'pull-xs-right text-muted').append('a').attr('class', 'toggle-all');

      var all_canvas = all_container.append('div').attr('class', 'all-container');
      this.updateAllCanvas();

      var all_data = this.data.sort(function(lhs, rhs) { return (lhs.label || "").toString().localeCompare((rhs.label || "").toString()); })

      app.visualizationRenderers.heatmap(all_canvas, all_data, {
        metric: this.metric,
        axis: this.axis,
        supplementaryData: this.supplementaryData,
      })

    }

    return this;
  },

  renderVisualization: function(canvas, data) {
    var renderer = 'bars';
    if (this.axis == 'site') {
      renderer = 'pies';
    }
    app.visualizationRenderers[renderer](canvas, data, {
      metric: this.metric,
      axis: this.axis,
      supplementaryData: this.supplementaryData,
      start_index: this.page * this.limit,
      stop_index: (this.page + 1) * this.limit,
    })
  },

  events: {
    'click [data-scope-filter]': 'refineFilter',
    'click .paging-button': 'movePage',
    'click .toggle-all': 'toggleAll',
    'click .increase-limit-button': 'increaseLimit',
  },

  refineFilter: function(event) {
    app.scope.set(this.axis, event.target.getAttribute('data-scope-filter'));
  },

  movePage: function(event) {
    this.page = parseInt(event.target.getAttribute('data-page-target'));
    this.render();
  },

  toggleAll: function(event) {
    this.showAll = !this.showAll;
    this.updateAllCanvas();
  },

  updateAllCanvas: function() {
    var display_button = this.$('.toggle-all');
    if (this.showAll) {
      display_button.text('Hide all');
    } else {
      display_button.text('Show all');
    }
    this.$('.all-container').toggle(this.showAll);
  },

  increaseLimit: function(event) {
    this.limit = parseInt(event.target.getAttribute('data-limit-target'));
    this.render();
  },

});
