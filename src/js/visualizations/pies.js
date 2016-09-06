app = app || {};
app.visualizationRenderers = app.visualizationRenderers || {};

app.visualizationRenderers.pies = function(canvas, data, metric, axis, supplementaryData) {

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



  var labels = data.map(function(d) { return d['label'] })

  var chart_size = 200; // py
  var min_size = 20;

  canvas.attr('class', 'canvas-flex');

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
      return Math.sqrt(count / max_count) * (chart_size - min_size) + min_size + 'px';
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
      return app.format_axis_label(axis)(d.label);
    })
    .attr('data-scope-filter', function(d) {
      return d.label;
    })
  container.append('small')
    .attr('class', 'text-muted text-xs-center')
    .text(function(d) {
      if (axis == 'exitCode') {
        return (supplementaryData['exitCodes'] || {})[d['label']];
      } else {
        return "";
      }
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
        return app.format_value(metric)(count);
      }
    });

}
