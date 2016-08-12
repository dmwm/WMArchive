var app = app || {};
app.visualizationViews = app.visualizationViews || {};

app.visualizationViews['default'] = Backbone.View.extend({

  title: '',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'data', 'metric', 'axis'));
  },

  render: function() {
    this.$el.empty();

    var self = this;
    var data = this.data;
    data.sort(function(lhs, rhs) { return (lhs.label || "").localeCompare(rhs.label || ""); });

    // Preprocess data
    var is_stacked = (data[0] || {})['jobstates'] != null;

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

    if (data.length <= 5) {

      // Setup scales
      var length = d3.scaleLinear()
        .range([ 0, 100 ])
        .domain([ 0, max_count ]);

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

      var container = canvas.selectAll('.full-width-container')
        .data(stack_data)
        .enter()
          .append('div').attr('class', 'full-width-container');

      var bar = container.append('svg')
        .attr('class', 'chart')

      bar.selectAll('rect')
        .data(function(d) {
          return d;
        })
        .enter().append('rect')
          .attr('height', '100%')
          .attr('x', function(d) {
            return length(d[0]) + '%';
          })
          .attr('y', 0)
          .attr('width', function(d) {
            return (length(d[1]) - length(d[0])) + '%';
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

        var label = container.append('div')
          .attr('class', 'chart-label')

        label.append('a')
          .text(function(d) {
            return d.label;
          });
        label.append('text')
          .text(function(d) {
            var job_count = d[d.length - 1][1];
            return app.format_jobs(job_count) + ' (' + numeral(job_count / total_count).format('0.0%') + ')';
          });

    } else {

      var labels = data.map(function(d) { return d['label'] })

      var chart_size = 200; // py

      var canvas = d3.select(this.el)
        .attr('class', 'canvas-flex');

      var container = canvas.selectAll('.site-count-container')
        .data(data)
        .enter()
          .append('div').attr('class', 'site-count-container')
          .attr('style', 'max-width: ' + chart_size + 'px')

      var pie = container.append('svg')
        .attr('class', 'chart')
        .attr('height', chart_size + 'px')
        .attr('width', function(d) {
          var count = 0;
          if (is_stacked) {
            count = d3.sum(d['jobstates'].map(function(dd) { return dd['count'] }));
          } else {
            count = d.average;
          }
          return Math.sqrt(count / max_count) * chart_size + 'px';
        })
        .attr('viewBox', '0 0 100 100')
        .append("g")
          .attr("transform", 'translate(50,50)')

      if (is_stacked) {

        var pieLayout = d3.pie()
          .value(function(d) {
            return d.count;
          })
          .sort(null);
        var path = pie.selectAll('path')
          .data(function(d) {
            var dd = pieLayout(d.jobstates);
            return dd;
          })
          .enter()
            .append('path')
            .attr('d', d3.arc()
              .innerRadius(50 / 4)
              .outerRadius(50))
            .attr('fill', function(d, i) {
              switch (d.data.jobstate) {
                case 'success':
                  return '#31AD64';
                case 'jobfailed':
                  return '#E54E42';
                case 'submitfailed':
                  return '#3081B8';
                default:
                  return 'black';
              }
            })
            .attr('data-toggle', 'tooltip')
            .attr('title', function(d) {
              var job_count = d.data.count;
              var job_percentage = (d.endAngle - d.startAngle) / (2 * Math.PI);
              return d.data.jobstate + ': ' + app.format_jobs(job_count) + ' (' + numeral(job_percentage).format('0.0%') + ')';
            });
      } else {

        pie.append("circle").attr("r", "50").attr("fill", "black")

      }

      container.append('a')
        .attr('class', 'chart-label')
        .attr('style', 'max-width: ' + chart_size * 0.7 + 'px')
        .text(function(d) {
          return d['label'];
        })
      container.append('text')
        .attr('class', 'chart-label')
        .text(function(d) {
          var count = 0;
          if (is_stacked) {
            count = d3.sum(d['jobstates'].map(function(dd) { return dd['count'] }));
            return app.format_jobs(count) + ' (' + numeral(count / total_count).format('0.0%') + ')';
          } else {
            count = d.average;
            return app.format_value(self.metric)(count);
          }
        });


    }

    return this;
  },

});
