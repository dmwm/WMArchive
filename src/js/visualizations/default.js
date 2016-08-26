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
    data.sort(function(lhs, rhs) { return (lhs.label || "").toString().localeCompare((rhs.label || "").toString()); });

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

      var bar_data = data;

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
        bar_data = stack_data;
      }

      var container = canvas.selectAll('.full-width-container')
        .data(bar_data)
        .enter()
          .append('div').attr('class', 'full-width-container');

      var bar = container.append('svg')
        .attr('class', 'chart')

      if (is_stacked) {
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
        } else {
          bar.append("rect")
            .attr('height', '100%')
            .attr('width', function(d) {
              return length(d.average) + '%';
            })
            .attr('class', 'filled');
        }

        var label = container.append('div')
          .attr('class', 'chart-label')

        label.append('a')
          .text(function(d) {
            return d.label;
          });
        var label_text = label.append('text')
          .text(function(d) {
            var count = 0;
            if (is_stacked) {
              count = d[d.length - 1][1];
              return app.format_jobs(count) + ' (' + numeral(count / total_count).format('0.0%') + ')';
            } else {
              count = d.average;
              return app.format_value(self.metric)(count);
            }
          });
        if (!is_stacked) {
          label_text.attr('data-toggle', 'tooltip')
            .attr('title', function(d) {
              return "averaged over " + app.format_jobs(d.count);
            });
        }

    } else if (data.length <= 50) {

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

    } else {

      var item_size = 50 * 10 / Math.sqrt(data.length);

      // Setup scales
      var strength = d3.scaleLinear()
        .range([ 0, 1 ])
        .domain([ 0, max_count ]);

      var bar_data = data;

      if (is_stacked) {
        var stack = d3.stack()
            .keys(all_jobstates)
            .order(d3.stackOrderNone)
            .offset(d3.stackOffsetExpand);
        var stack_data = stack(data.map(function(d) {
          var r = {
            label: d.label,
            totalCount: d3.sum(d.jobstates.map(function(j) { return j.count; }))
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
          // console.log(col.data)
          new_row.average = col.data.totalCount;
          return new_row;
        });
        bar_data = stack_data;
      }

      var container = canvas.append('div').attr('class', 'heatmap')

      var item = container.selectAll('.heatmap-item')
        .data(bar_data)
        .enter()
          .append('svg').attr('class', 'heatmap-item')
          .attr('width', item_size)
          .attr('height', item_size)
      var item_content = item.append('g')
        .attr('data-toggle', 'tooltip')
        .attr('title', function(d) {
          return d.label + ": " + app.format_value(self.metric)(d.average);
        })
        .attr('opacity', function(d) {
          return strength(d.average);
        });


      if (is_stacked) {
        var y = d3.scaleLinear()
          .range([ 0, 100 ])
        item_content.selectAll('rect')
          .data(function(d) {
            return d;
          })
          .enter().append('rect')
            .attr('width', '100%')
            .attr('y', function(d) {
              return y(d[0]) + '%';
            })
            .attr('x', 0)
            .attr('height', function(d) {
              return (y(d[1]) - y(d[0])) + '%';
            })
            .attr('fill', function(d) {
              return color(d.jobstate);
            })
        } else {
          item_content.append("rect")
            .attr('height', '100%')
            .attr('width', '100%')
            .attr('class', 'filled')
        }

        item_content.append("rect")
          .attr('height', '100%')
          .attr('width', '100%')
          .attr('data-label', function(d) {
            return d.label;
          })
          .attr('opacity', 0)

    }

    return this;
  },

  events: {
    'click a': 'refineFilter',
    'click .heatmap-item': 'refineFilter',
  },

  refineFilter: function(event) {
    var label = event.target.text;
    if (label == null) {
      label = event.target.getAttribute('data-label');
    }
    app.scope.set(this.axis, label);
  },

});
