// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

app = app || {};
app.visualizationRenderers = app.visualizationRenderers || {};

app.visualizationRenderers.bars = function(canvas, data, options) {

  var metric = options.metric;
  var axis = options.axis;
  var supplementaryData = options.supplementaryData;

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

  if (options.start_index != null && options.stop_index != null) {
    bar_data = bar_data.slice(options.start_index, options.stop_index);
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
        return app.format_axis_label(axis)(d.label);
      })
      .attr('data-scope-filter', function(d) {
        return d.label;
      });
    label.append('small')
      .attr('class', 'text-muted')
      .text(function(d) {
        if (axis == 'exitCode') {
          return (supplementaryData['exitCodes'] || {})[d['label']];
        } else {
          return "";
        }
      })

    var label_text = label.append('text')
      .text(function(d) {
        var count = 0;
        if (is_stacked) {
          count = d[d.length - 1][1];
          return app.format_jobs(count) + ' (' + numeral(count / total_count).format('0.0%') + ')';
        } else {
          count = d.average;
          return app.format_value(metric)(count);
        }
      });
    if (!is_stacked) {
      label_text.attr('data-toggle', 'tooltip')
        .attr('title', function(d) {
          return "averaged over " + app.format_jobs(d.count);
        });
    }

}
