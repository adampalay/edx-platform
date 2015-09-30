(function(define) {
'use strict';
define('video/09_play_pause_control.js', [], function() {
    /**
     * Play/pause control module.
     * @exports video/09_play_pause_control.js
     * @constructor
     * @param {Object} state The object containing the state of the video
     * @param {Object} i18n The object containing strings with translations.
     * @return {jquery Promise}
     */
    var PlayPauseControl = function(state, i18n) {
        if (!(this instanceof PlayPauseControl)) {
            return new PlayPauseControl(state, i18n);
        }

        _.bindAll(this, 'play', 'pause', 'onClick', 'destroy');
        this.state = state;
        this.state.videoPlayPauseControl = this;
        this.i18n = i18n;
        this.initialize();

        return $.Deferred().resolve().promise();
    };

    PlayPauseControl.prototype = {
        template: [
            '<button class="control video_control play" aria-disabled="false" aria-pressed="false">',
                '<span class="icon-fallback-img">',
                    '<span class="icon fa fa-play" aria-hidden="true"></span>',
                    '<span class="text control-text">',
                        gettext('Play'),
                    '</span>',
                '</span>',
            '</button>'
        ].join(''),

        destroy: function () {
            this.el.remove();
            this.state.el.off('destroy', this.destroy);
            delete this.state.videoPlayPauseControl;
        },

        /** Initializes the module. */
        initialize: function() {
            this.el = $(this.template);
            this.render();
            this.bindHandlers();
        },

        /**
         * Creates any necessary DOM elements, attach them, and set their,
         * initial configuration.
         */
        render: function() {
            this.state.el.find('.vcr').prepend(this.el);
        },

        /** Bind any necessary function callbacks to DOM events. */
        bindHandlers: function() {
            this.el.on({
                'click': this.onClick
            });
            this.state.el.on({
                'play': this.play,
                'pause ended': this.pause,
                'destroy': this.destroy
            });
        },

        onClick: function (event) {
            event.preventDefault();
            this.state.videoCommands.execute('togglePlayback');
        },

        play: function () {
            this.el
                .attr('aria-pressed', 'true')
                .addClass('pause')
                .removeClass('play')
                .find('.icon')
                    .removeClass('fa-play')
                    .addClass('fa-pause')
                .find('.control-text')
                    .text(this.i18n['Pause']);
        },

        pause: function () {
            this.el
                .attr('aria-pressed', 'false')
                .removeClass('pause')
                .addClass('play')
                .find('.icon')
                    .removeClass('fa-pause')
                    .addClass('fa-play')
                .find('.control-text')
                    .text(this.i18n['Play']);
        }
    };

    return PlayPauseControl;
});
}(RequireJS.define));
