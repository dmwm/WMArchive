# Performance UI architecture

- The performance UI is accessible at the endpoint `/web/performance`, e.g.

  [https://cmsweb.cern.ch/wmarchive/web/performance](https://cmsweb.cern.ch/wmarchive/web/performance)

  or on the test deployment:

  [https://cmsweb-testbed.cern.ch/wmarchive/web/performance](https://cmsweb-testbed.cern.ch/wmarchive/web/performance)

## Dependencies

**Primary architecture:**
- **[Backbone.js](http://backbonejs.org)** for primary JavaScript MVC architecture.

  The **[Underscore.js](http://underscorejs.org)** library is a hard dependency of Backbone.js for templating and JavaScript functionality.
- The **[Sass](http://sass-lang.com)** CSS preprocessor.
- The **[Bootstrap](http://getbootstrap.com)** CSS framework for the responsive grid and many UI components.

  In particular the [v4.0 alpha](http://v4-alpha.getbootstrap.com) version of Bootstrap that is based on Sass.
- **[D3.js](https://d3js.org)** for data visualization in HTML in SVG.

**Secondary components:**
- **[jQuery](http://jquery.com)** for JavaScript functionality.
- **[Moment.js](http://momentjs.com)** for JavaScript date handling.
- **[Numeral.js](http://numeraljs.com)** for JavaScript human-readable formatting.
- **[Tether](http://tether.io)** for Bootstrap popovers tooltips.
- The **[Daterangepicker](https://github.com/dangrossman/bootstrap-daterangepicker)** component to implement the timeframe picker.
- The **[Twitter Typeahead & Bloodhound frameworks](https://github.com/twitter/typeahead.js)** for dropdown suggestions in scope filters

## Architecture overview

- The UI is primarily based on the [Backbone.js](http://backbonejs.org) JavaScript MVC framework. This framework provides a lightweight architecture to build a JavaScript UI upon the existing `WMCore` server backend.
- The implementation focuses on a single stateful `Scope` model object that holds the scope filter values the user has entered and a collection of `Visualizations` that is constructed from all combinations of the selected metrics and axes. These models are implemented in `WMArchive/src/js/models/scope.js` and `WMArchive/src/js/models/visualizations.js` and created in `WMArchive/src/templates/index.html`.
- Views displayed on the site, such as the timeframe picker and the scope filters, reflect the state of the `app.scope` and `app.visualizations` singletons and respond to changes of their attributes.

They usually listen to changes in their `initialize` function and then `render` their state. Consider for example the implementation in `WMArchive/src/js/views/filter.js` that implements the view for the scope filter input fields, and the way this view is handled in its superview `src/js/views/scope.js` to understand this architecture.
- The root view of the performance UI is implemented in `WMArchive/src/js/views/performance.js` and is rendered by `WMArchive/src/js/routers/router.js` when the router navigates to `/web/performance`.
- The model objects fetch their data from `data/performance` and re-fetch whenever a relevant attribute changes. They also handle canceling pending fetches when initiating a re-fetch. Consider for example `WMArchive/src/js/models/visualization.js` and the comments therein to understand the fetch process and how to hook into it.
- The list of visualizations is constructed dynamically in `WMArchive/src/js/models/visualizations.js`. The `WMArchive/src/js/views/performance.js` view observes this collection and renders a widget for each visualization. The widget is responsible for presenting the frame, a loading indicator and error information. It then delegates the rendering of its content when data is available to the `WMArchive/src/js/visualizations/visualization.js` view. The visualization view decides how to render the data depending on options such as the number of data points and the specific metric and axis. It makes use of the various visualization renderers in  `WMArchive/src/js/visualizations/` to finally render the data and provides interactive UI elements to navigate the data.
- Each visualization renderer in `WMArchive/src/js/visualizations/` is implemented as a function with the same signature that takes a canvas element, data and options and renders the data to the canvas.

  The three visualizations renderers implemented so far, namely `bars.js`, `pies.js` and `heatmap.js` are based upon the [D3.js](https://d3js.org) data visualization framework to construct HTML and SVG trees from the data. Refer to the [D3.js documentation](https://github.com/d3/d3/wiki) and [examples](http://bl.ocks.org/mbostock) to understand the rendering procedure. Setting up useful [scales](https://github.com/d3/d3/blob/master/API.md#scales-d3-scale) is everything in D3.js!

  Note that you can use any visualization framework of your choice to render the data. I preferred D3.js to implement highly customized visualizations, since this is one of the reason why we implement the UI based on a JavaScript architecture [instead of a data visualization dashboard](../002_2016-07-15.md#elasticsearch-and-kibana). You may however also use frameworks such as [Chart.js](http://www.chartjs.org) for quick visualizations.

## CSS preprocessing with Sass

- CSS stylesheets in `WMArchive/src/css` are not written directly but they are generated from the [Sass](http://sass-lang.com) files in `WMArchive/src/sass`. [Sass](http://sass-lang.com) is a CSS preprocessor: Every CSS file is also valid Sass syntax, but Sass adds a lot of very important functionality to CSS such as variables and nested selectors.
- The `WMArchive/src/sass/main.scss` is compiled to the `WMArchive/src/css/main.css` file that is then a regular CSS file and included in `WMArchive/src/templates/index.html`. Sass dependencies such as Bootstrap are imported in `main.scss` and thus included in the resulting CSS file. This has the advantage that the default values in Bootstrap can be overridden in `main.scss` to specify consistent colors and other attributes.
- The usual workflow is to edit the `WMArchive/src/sass/main.scss` and automatically compile the SASS file on every save, thus continuously updating the `WMArchive/src/css/main.css` stylesheet. For this you need to [install Sass](http://sass-lang.com/install) and then run:

  ```
  sass --watch src/sass/main.scss:src/css/main.css
  ```

  You can also compile a Sass file without watching it:

  ```
  sass src/sass/main.scss src/css/main.css
  ```
- Always commit both the source Sass file as well as the derived CSS file to the repository.
