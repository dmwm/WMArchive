var app = app || {};
app.visualizationViews = app.visualizationViews || {};

app.visualizationViews['time'] = Backbone.View.extend({

  initialize: function(options) {
    _.extend(this, _.pick(options, 'data', 'metric', 'axis'));
    $(window).on('resize.resizeview', this.resize.bind(this));
  },

  render: function() {
    this.$el.empty();

    var self = this;
    var data = this.data;
    data.sort(function(lhs, rhs) { return new Date(rhs.label.start_date) - new Date(lhs.label.start_date) });

    // Preprocess data
    var is_stacked = (data[0] || {})['jobstates'] != null;

    var all_dates = [...data.map(function(d) { return new Date(d.label.start_date) }), ...data.map(function(d) { return new Date(d.label.end_date) })];

    if (is_stacked) {
      var all_jobstates = new Set();
      for (var d of data) {
        for (var dd of d.jobstates) {
          all_jobstates.add(dd.jobstate);
        }
      }
      all_jobstates = Array.from(all_jobstates);

      var color = d3.scaleOrdinal()
        .range(all_jobstates.map(function(d) {
          switch (d) {
            case 'success':
              return '#31AD64';
            case 'jobfailed':
              return '#E54E42';
            case 'submitfailed':
              return '#3081B8';
            default:
              return 'black';
          }
        }))
        .domain(all_jobstates);
    }

    var axis_counts = data.map(function(d) {
      if (is_stacked) {
        return d3.sum(d.jobstates, function(dd) { return dd.count; })
      } else {
        return d.average;
      }
    });
    var max_count = d3.max(axis_counts);
    var total_count = d3.sum(axis_counts);


    var canvas = d3.select(this.el);
    var container = canvas.append('div').attr('class', 'full-width-container')
      .append('svg')
        .attr('height', '200px')
        .attr('width', '100%')
    var margin = { top: 20, right: 40, bottom: 30, left: 60 },
      height = parseInt(container.style('height'), 10)
      width = parseInt(container.style('width'), 10);
    var chart = container.append('g').attr("transform", "translate(" + margin.left + "," + margin.top + ")")


    // Setup scales
    var length = d3.scaleLinear()
      .range([ height - margin.top - margin.bottom, 0 ])
      .domain([ 0, max_count ])
      .nice();
    var position = d3.scaleUtc()
      .domain([ d3.min(all_dates), d3.max(all_dates) ])
      .range([ 0, width - margin.left - margin.right ])
      .nice();

    if (is_stacked) {
      var stack = d3.stack()
          .keys(all_jobstates)
          .order(d3.stackOrderNone)
          .offset(d3.stackOffsetNone);
      var stack_data = stack(data.map(function(d) {
        var r = {
          label: d.label,
        };
        for (var jobstate of all_jobstates) {
          r[jobstate] = 0;
        }
        for (var dd of d.jobstates) {
          r[dd.jobstate] = dd.count;
        }
        return r;
      }));
      stack_data = stack_data[0].map(function(col, i) {
        var new_row = stack_data.map(function(row) {
          var new_col = row[i];
          new_col.jobstate = row.key;
          return new_col;
        });
        new_row.label = col.data.label;
        return new_row;
      });

      var bar = chart.selectAll('g')
        .data(stack_data)
        .enter()
          .append('g')
          .attr('transform', function(d) {
            return 'translate(' + position(new Date(d.label.start_date)) + ',0)';
          });

      bar.append('g').selectAll('rect')
        .data(function(d) {
          return d;
        })
        .enter().append('rect')
          .attr('width', function(d) {
            return position(new Date(d.data.label.end_date)) - position(new Date(d.data.label.start_date));
          })
          .attr('y', function(d) {
            return length(d[1]);
          })
          .attr('x', 0)
          .attr('height', function(d) {
            return (length(d[0]) - length(d[1]));
          })
          .attr('fill', function(d) {
            return color(d.jobstate);
          })
          .attr('data-toggle', 'tooltip')
          .attr('title', function(d) {
            var job_count = d[1] - d[0];
            return d.jobstate + ': ' + app.format_jobs(job_count) + ' (' + numeral(job_count / d3.sum(all_jobstates.map(function(jobstate) {
              return d.data[jobstate];
            }))).format('0.0%') + ')';
          });
      } else {

        var line = d3.line()
          .x(function(d) { return position(d3.timeHour.offset(new Date(d.label.start_date), 12)) })
          .y(function(d) { return length(d.average) })

        var area = d3.area()
          .x(line.x())
          .y1(line.y())
          .y0(length(0))

        chart.append("path")
          .datum(data)
          .attr("class", "area")
          .attr("d", area)
        chart.append("path")
          .datum(data)
          .attr("class", "line")
          .attr("d", line)

        chart.selectAll(".dot")
          .data(data)
        .enter().append("circle")
          .attr("class", "dot")
          .attr("cx", line.x())
          .attr("cy", line.y())
          .attr("r", 3.5)
          .attr('data-toggle', 'tooltip')
          .attr('title', function(d) {
            return d3.timeFormat(app.format_time_d3)(new Date(d.label.start_date)) + ": " + app.format_value(self.metric)(d.average);
          });

      }

      chart.append("g")
        .attr("class", "axis axis-h")
        .attr("transform", "translate(0," + (height - margin.top - margin.bottom) + ")")
        .call(d3.axisBottom(position).ticks(Math.round((width - margin.left - margin.right)/100)));
      chart.append("g")
        .attr("class", "axis axis-v")
        .attr("transform", "translate(" + 0 + ",0)")
        .call(d3.axisLeft(length).ticks(3).tickFormat(app.format_tick(self.metric)))
        .append("g")
          .attr("class", "label")
          .attr("transform", "translate(0," + 0 + ")")
          .append("text")
            .attr("x", -9)
            .attr("y", -10)
            .attr("fill", "black")
            .text(app.format_ticks_label(self.metric))

    return this;
  },

  resize: function() {
    this.render();
	},

  events: {
    'click a': 'refineFilter',
  },

  refineFilter: function(event) {
    app.scope.set(this.axis, event.target.text);
  },

});
