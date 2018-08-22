if (requireNamespace('lintr', quietly = TRUE)) {
  context('lints')

  test_that('Package Style', {
    # HACK(mgnb): something about the way codecov runs causes it to miss the
    # .lintr file. For now, I am disabling these on travis, though the place
    # to really look is the codecov() call in .travis.yml
    skip_on_travis()
    lintr::expect_lint_free()
  })
}
