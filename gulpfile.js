var gulp = require('gulp');
var eslint = require('gulp-eslint');

gulp.task('lint', function() {
  return gulp.src(['./bin/tails3', './lib/**/*.js', './*.js'])
    .pipe(eslint('eslint.json'))
    .pipe(eslint.format());
});

gulp.task('default', ['lint']);
